package main

import shared "../shared"
import "core:flags"
import "core:log"
import "core:nbio"
import "core:net"
import "core:os"
import "core:prof/spall"
import "core:sync"
import "core:sync/chan"
import "core:thread"

DEFAULT_WORKER_COUNT :: 14
DEFAULT_IO_THREAD_COUNT :: 4
DEFAULT_WORK_QUEUE_SIZE :: 8192
RECV_BUFFER_SIZE :: 4096
DEFAULT_PROFILE_PATH :: "profiles/server.spall"
PROFILE_BUFFER_SIZE :: 64 * 1024
PROFILE_FLUSH_EVENTS :: 512
SPALL_PROFILE :: #config(SPALL_PROFILE, false)

Server_Options :: struct {
	workers:      int    `usage:"Number of request worker threads."`,
	io_threads:   int    `args:"name=io-threads" usage:"Number of IO event-loop threads for accepted clients."`,
	queue_size:   int    `args:"name=queue-size" usage:"Request work queue capacity."`,
	profile:      bool   `usage:"Write a Spall trace. Requires build with -define:SPALL_PROFILE=true."`,
	profile_path: string `args:"name=profile-path" usage:"Spall trace output path."`,
}

Server_State :: struct {
	work_sender: chan.Chan(^Work_Item, .Send),
	next_conn_id: u64,
	io_threads: []IO_Thread_State,
	next_io_index: int,
}

IO_Thread_State :: struct {
	index: int,
	loop: ^nbio.Event_Loop,
	work_sender: chan.Chan(^Work_Item, .Send),
	ready_sender: chan.Chan(^IO_Thread_State, .Send),
}

Accepted_Client :: struct {
	id: u64,
	socket: net.TCP_Socket,
	source: net.Endpoint,
	work_sender: chan.Chan(^Work_Item, .Send),
}

Connection :: struct {
	id:            u64,
	socket: net.TCP_Socket,
	source: net.Endpoint,
	loop: ^nbio.Event_Loop,
	work_sender: chan.Chan(^Work_Item, .Send),
	recv_buf: [RECV_BUFFER_SIZE]byte,
	line_buf: [shared.MAX_LINE_BYTES]byte,
	line_len: int,
	pending_lines: [dynamic][]byte,
	busy: bool,
	waiting_for_queue: bool,
	retry_queued: bool,
	closed: bool,
}

Work_Item :: struct {
	connection: ^Connection,
	line: []byte,
}

Response_Item :: struct {
	connection: ^Connection,
	data: []byte,
}

when SPALL_PROFILE {
	profile_ctx: spall.Context
	profile_active: bool
	@(thread_local) profile_buffer: spall.Buffer
	@(thread_local) profile_event_count: int
}

ENEMY_BASES := []shared.Enemy_Base_View {
	{id = 1, x = 180, y = 160, level = 2, name = "Stone Reef"},
	{id = 2, x = 420, y = 260, level = 4, name = "Iron Cove"},
	{id = 3, x = 660, y = 180, level = 6, name = "Storm Pier"},
	{id = 4, x = 540, y = 420, level = 8, name = "Crab Harbor"},
}

main :: proc() {
	context.logger = log.create_console_logger()
	options := Server_Options {
		workers      = DEFAULT_WORKER_COUNT,
		io_threads   = DEFAULT_IO_THREAD_COUNT,
		queue_size   = DEFAULT_WORK_QUEUE_SIZE,
		profile_path = DEFAULT_PROFILE_PATH,
	}
	flags.parse_or_exit(&options, os.args, .Unix)

	if options.workers <= 0 || options.io_threads <= 0 || options.queue_size <= 0 {
		log.panic("workers, io-threads, and queue-size must be greater than zero")
	}
	if options.profile {
		when SPALL_PROFILE {
			profile_start(options.profile_path)
			_ = profile_thread_start("accept loop")
		} else {
			log.panic("profile requested, but server was built without -define:SPALL_PROFILE=true")
		}
	}

	endpoint := net.Endpoint {
		address = net.IP4_Loopback,
		port    = shared.SERVER_PORT,
	}
	if err := nbio.acquire_thread_event_loop(); err != nil {
		log.panic("event loop init failed:", err)
	}
	defer nbio.release_thread_event_loop()

	listener, listen_err := nbio.listen_tcp(endpoint)
	if listen_err != nil {
		log.panic("server listen failed", listen_err)
	}
	defer nbio.close(listener)

	work_queue, queue_err := chan.create_buffered(chan.Chan(^Work_Item), options.queue_size, context.allocator)
	if queue_err != .None {
		log.panic("work queue create failed:", queue_err)
	}
	defer chan.destroy(work_queue)
	work_sender := chan.as_send(work_queue)
	work_receiver := chan.as_recv(work_queue)

	for i in 0 ..< options.workers {
		t := thread.create_and_start_with_poly_data2(
			i,
			work_receiver,
			work_worker,
			context,
			.Normal,
			true,
		)
		if t == nil {
			log.panic("failed to start request worker:", i)
		}
	}

	ready_queue, ready_err := chan.create_buffered(chan.Chan(^IO_Thread_State), options.io_threads, context.allocator)
	if ready_err != .None {
		log.panic("io ready queue create failed:", ready_err)
	}
	defer chan.destroy(ready_queue)
	ready_sender := chan.as_send(ready_queue)
	ready_receiver := chan.as_recv(ready_queue)

	io_threads := make([]IO_Thread_State, options.io_threads)
	for i in 0 ..< options.io_threads {
		io_threads[i] = IO_Thread_State {
			index = i,
			work_sender = work_sender,
			ready_sender = ready_sender,
		}

		t := thread.create_and_start_with_poly_data(
			&io_threads[i],
			io_thread_proc,
			context,
			.Normal,
			true,
		)
		if t == nil {
			log.panic("failed to start io thread:", i)
		}
	}

	for i in 0 ..< options.io_threads {
		io_thread, ok := chan.recv(ready_receiver)
		if !ok || io_thread == nil || io_thread.loop == nil {
			log.panic("io thread failed to start:", i)
		}
	}

	log.info(
		"server listening on",
		shared.SERVER_ADDRESS,
		"accept_threads",
		1,
		"io_threads",
		options.io_threads,
		"workers",
		options.workers,
		"queue_size",
		options.queue_size,
	)

	state := Server_State {
		work_sender = work_sender,
		next_conn_id = 1,
		io_threads = io_threads,
	}
	nbio.accept_poly(listener, &state, on_accept)

	for {
		when SPALL_PROFILE {
			profile_begin("accept.tick")
		}
		err := nbio.tick()
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil {
			log.panic("event loop tick failed:", err)
		}
	}
}

io_thread_proc :: proc(state: ^IO_Thread_State) {
	when SPALL_PROFILE {
		_ = profile_thread_start("io event loop")
	}

	if err := nbio.acquire_thread_event_loop(); err != nil {
		log.panic("io event loop init failed:", state.index, err)
	}
	defer nbio.release_thread_event_loop()

	state.loop = nbio.current_thread_event_loop()
	if err := nbio.tick(0); err != nil {
		log.panic("io event loop startup tick failed:", state.index, err)
	}
	if !chan.send(state.ready_sender, state) {
		log.panic("io thread ready signal failed:", state.index)
	}

	log.info("io thread started:", state.index)
	for {
		when SPALL_PROFILE {
			profile_begin("io.tick")
		}
		err := nbio.tick()
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil {
			log.panic("io event loop tick failed:", state.index, err)
		}
	}
}

on_accept :: proc(op: ^nbio.Operation, state: ^Server_State) {
	when SPALL_PROFILE {
		profile_begin("io.accept")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if op.accept.err != nil {
		log.warn("accept failed:", op.accept.err)
		nbio.accept_poly(op.accept.socket, state, on_accept)
		return
	}

	nbio.accept_poly(op.accept.socket, state, on_accept)

	io_thread := &state.io_threads[state.next_io_index]
	state.next_io_index = (state.next_io_index + 1) % len(state.io_threads)

	accepted := new(Accepted_Client)
	accepted.id = state.next_conn_id
	state.next_conn_id += 1
	accepted.socket = op.accept.client
	accepted.source = op.accept.client_endpoint
	accepted.work_sender = state.work_sender

	nbio.next_tick_poly(accepted, on_client_assigned, l=io_thread.loop)
}

on_client_assigned :: proc(op: ^nbio.Operation, accepted: ^Accepted_Client) {
	when SPALL_PROFILE {
		profile_begin("io.client_assigned")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if accepted == nil {
		return
	}
	defer free(accepted)

	if err := nbio.associate_socket(accepted.socket, l=op.l); err != nil {
		log.warn("client socket association failed:", err)
		net.close(accepted.socket)
		return
	}

	connection := new(Connection)
	connection.id = accepted.id
	connection.socket = accepted.socket
	connection.source = accepted.source
	connection.loop = op.l
	connection.work_sender = accepted.work_sender
	connection.pending_lines = make([dynamic][]byte)

	log.info("client connected:", connection.source)
	schedule_recv(connection)
}

work_worker :: proc(index: int, jobs: chan.Chan(^Work_Item, .Recv)) {
	when SPALL_PROFILE {
		_ = profile_thread_start("request worker")
	}

	log.info("request worker started:", index)
	for {
		when SPALL_PROFILE {
			profile_begin("worker.recv_wait")
		}
		work, ok := chan.recv(jobs)
		when SPALL_PROFILE {
			profile_end()
		}
		if !ok {
			return
		}

		when SPALL_PROFILE {
			profile_begin("worker.process")
		}
		response := new(Response_Item)
		response.connection = work.connection
		response.data = process_request(work.line)

		delete(work.line)
		free(work)
		when SPALL_PROFILE {
			profile_end()
		}

		nbio.next_tick_poly(response, on_response_ready, l=response.connection.loop)
	}
}

schedule_recv :: proc(connection: ^Connection) {
	if connection.closed || connection.busy || connection.waiting_for_queue {
		return
	}
	nbio.recv_poly(connection.socket, {connection.recv_buf[:]}, connection, on_recv, l=connection.loop)
}

on_recv :: proc(op: ^nbio.Operation, connection: ^Connection) {
	when SPALL_PROFILE {
		profile_begin("io.recv")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if connection.closed {
		return
	}

	if op.recv.err != nil || op.recv.received == 0 {
		close_connection(connection)
		return
	}

	if !append_received_bytes(connection, connection.recv_buf[:op.recv.received]) {
		close_connection(connection)
		return
	}

	dispatch_next_line(connection)
	if !connection.closed && !connection.busy {
		schedule_recv(connection)
	}
}

append_received_bytes :: proc(connection: ^Connection, data: []byte) -> bool {
	when SPALL_PROFILE {
		profile_begin("io.parse_lines")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	for b in data {
		if b == '\n' {
			line := make([]byte, connection.line_len)
			copy(line, connection.line_buf[:connection.line_len])
			append(&connection.pending_lines, line)
			connection.line_len = 0
			continue
		}

		if connection.line_len >= len(connection.line_buf) {
			return false
		}
		connection.line_buf[connection.line_len] = b
		connection.line_len += 1
	}
	return true
}

dispatch_next_line :: proc(connection: ^Connection) {
	when SPALL_PROFILE {
		profile_begin("queue.dispatch")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if connection.closed || connection.busy || len(connection.pending_lines) == 0 {
		return
	}

	line := connection.pending_lines[0]
	work := new(Work_Item)
	work.connection = connection
	work.line = line

	if !chan.try_send(connection.work_sender, work) {
		free(work)
		connection.waiting_for_queue = true
		schedule_dispatch_retry(connection)
		return
	}

	_ = pop_front(&connection.pending_lines)
	connection.busy = true
	connection.waiting_for_queue = false
}

schedule_dispatch_retry :: proc(connection: ^Connection) {
	if connection.closed || connection.retry_queued {
		return
	}

	connection.retry_queued = true
	nbio.next_tick_poly(connection, on_dispatch_retry, l=connection.loop)
}

on_dispatch_retry :: proc(op: ^nbio.Operation, connection: ^Connection) {
	if connection.closed {
		return
	}

	connection.retry_queued = false
	connection.waiting_for_queue = false
	dispatch_next_line(connection)
	if !connection.closed && !connection.busy && !connection.waiting_for_queue {
		schedule_recv(connection)
	}
}

process_request :: proc(line: []byte) -> []byte {
	when SPALL_PROFILE {
		profile_begin("request.process")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	when SPALL_PROFILE {
		profile_begin("json.decode_envelope")
	}
	envelope, err := shared.decode_envelope(line)
	when SPALL_PROFILE {
		profile_end()
	}
	if err != nil {
		return encode_json_line(shared.make_error_response(0, "invalid JSON message"))
	}

	#partial switch envelope.kind {
	case .Get_World_Map:
		request: shared.Get_World_Map_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil {
			return encode_json_line(shared.make_error_response(envelope.seq, "invalid world map request"))
		}
		return encode_json_line(shared.make_world_map_response(request.seq, ENEMY_BASES))

	case .Select_Base:
		request: shared.Select_Base_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil {
			return encode_json_line(shared.make_error_response(envelope.seq, "invalid select base request"))
		}
		return encode_json_line(shared.make_error_response(request.seq, "battle screen is not implemented yet"))

	case:
		return encode_json_line(shared.make_error_response(envelope.seq, "unknown message kind"))
	}
}

encode_json_line :: proc(message: any) -> []byte {
	when SPALL_PROFILE {
		profile_begin("json.encode_line")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	data, err := shared.encode_json(message)
	if err != nil {
		return nil
	}
	defer delete(data)

	line := make([]byte, len(data) + 1)
	copy(line, data)
	line[len(data)] = '\n'
	return line
}

on_response_ready :: proc(op: ^nbio.Operation, response: ^Response_Item) {
	when SPALL_PROFILE {
		profile_begin("io.response_ready")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	connection := response.connection
	if connection.closed || response.data == nil {
		delete(response.data)
		free(response)
		if connection != nil && !connection.closed {
			close_connection(connection)
		}
		return
	}

	nbio.send_poly(connection.socket, {response.data}, response, on_sent, l=connection.loop)
}

on_sent :: proc(op: ^nbio.Operation, response: ^Response_Item) {
	when SPALL_PROFILE {
		profile_begin("io.sent")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	connection := response.connection
	delete(response.data)
	free(response)

	if connection.closed {
		return
	}

	if op.send.err != nil {
		close_connection(connection)
		return
	}

	connection.busy = false
	dispatch_next_line(connection)
	if !connection.closed && !connection.busy {
		schedule_recv(connection)
	}
}

close_connection :: proc(connection: ^Connection) {
	when SPALL_PROFILE {
		profile_begin("io.close")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if connection == nil || connection.closed {
		return
	}

	connection.closed = true
	for line in connection.pending_lines {
		delete(line)
	}
	delete(connection.pending_lines)
	nbio.close(connection.socket)
	log.info("client disconnected")
	free(connection)
}

when SPALL_PROFILE {
	profile_start :: proc(path: string) {
		if path == "" {
			log.panic("profile-path cannot be empty")
		}
		if err := os.make_directory_all("profiles"); err != nil && err != .Exist {
			log.panic("profile directory create failed:", err)
		}

		ctx, ok := spall.context_create_with_scale(path, false, 1)
		if !ok {
			log.panic("spall context create failed:", path)
		}

		profile_ctx = ctx
		profile_active = true
		log.info("spall profiling enabled:", path)
	}

	profile_stop :: proc() {
		if !profile_active {
			return
		}

		spall.context_destroy(&profile_ctx)
		profile_active = false
	}

	profile_thread_start :: proc(name: string) -> []byte {
		if !profile_active {
			return nil
		}

		backing := make([]byte, PROFILE_BUFFER_SIZE)
		buffer, ok := spall.buffer_create(backing, u32(sync.current_thread_id()))
		if !ok {
			log.panic("spall buffer create failed")
		}

		profile_buffer = buffer
		profile_event_count = 0
		spall._buffer_name_thread(&profile_ctx, &profile_buffer, name)
		return backing
	}

	profile_thread_stop :: proc(backing: []byte) {
		if !profile_active || len(profile_buffer.data) == 0 {
			return
		}

		spall.buffer_destroy(&profile_ctx, &profile_buffer)
		profile_buffer = spall.Buffer{}
		profile_event_count = 0
		if len(backing) > 0 {
			delete(backing)
		}
	}

	profile_begin :: proc(name: string) {
		if profile_active && len(profile_buffer.data) > 0 {
			spall._buffer_begin(&profile_ctx, &profile_buffer, name)
		}
	}

	profile_end :: proc() {
		if profile_active && len(profile_buffer.data) > 0 {
			spall._buffer_end(&profile_ctx, &profile_buffer)
			profile_event_count += 1
			if profile_event_count >= PROFILE_FLUSH_EVENTS {
				spall.buffer_flush(&profile_ctx, &profile_buffer)
				profile_event_count = 0
			}
		}
	}
}
