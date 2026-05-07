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
import "core:sys/posix"
import "core:thread"
import "core:time"

DEFAULT_WORKER_COUNT :: 14
DEFAULT_IO_THREAD_COUNT :: 1
DEFAULT_WORK_QUEUE_SIZE :: 8192
RECV_BUFFER_SIZE :: 4096
WORKER_IDLE_SLEEP :: 50 * time.Microsecond
DEFAULT_IO_TICK_TIMEOUT_US :: -1
DEFAULT_IO_QUIESCE_ROUNDS :: 8
DEFAULT_METRICS_INTERVAL_MS :: 1000
DEFAULT_PROFILE_PATH :: "profiles/server.spall"
PROFILE_BUFFER_SIZE :: 64 * 1024
PROFILE_FLUSH_EVENTS :: 512
SPALL_PROFILE :: #config(SPALL_PROFILE, false)

Server_Options :: struct {
	workers:      int    `usage:"Number of request worker threads."`,
	io_threads:   int    `args:"name=io-threads" usage:"Number of IO event-loop threads for accepted clients."`,
	queue_size:   int    `args:"name=queue-size" usage:"Request work queue capacity per IO shard."`,
	io_tick_timeout_us: int `args:"name=io-tick-timeout-us" usage:"IO loop tick timeout in microseconds. -1 blocks until socket or explicit wake."`,
	io_quiesce_rounds: int `args:"name=io-quiesce-rounds" usage:"Maximum non-blocking IO drain rounds after each blocking tick. Zero disables quiescing."`,
	steal_work:   bool   `args:"name=steal-work" usage:"Allow idle workers to scan other IO queues for work."`,
	metrics:      bool   `usage:"Log per-IO useful/empty tick metrics."`,
	metrics_ms:   int    `args:"name=metrics-ms" usage:"Per-IO metrics log interval in milliseconds."`,
	profile:      bool   `usage:"Write a Spall trace. Requires build with -define:SPALL_PROFILE=true."`,
	profile_path: string `args:"name=profile-path" usage:"Spall trace output path."`,
}

Server_State :: struct {
	next_conn_id: u64,
	io_threads: []IO_Thread_State,
	next_io_index: int,
}

Work_Queue_State :: struct {
	index: int,
	sender: chan.Chan(^Work_Item, .Send),
	receiver: chan.Chan(^Work_Item, .Recv),
}

Worker_State :: struct {
	index: int,
	home_queue: int,
	work_queues: []Work_Queue_State,
	steal_work: bool,
}

IO_Thread_State :: struct {
	index: int,
	loop: ^nbio.Event_Loop,
	tick_timeout: time.Duration,
	quiesce_rounds: int,
	work_sender: chan.Chan(^Work_Item, .Send),
	accepted_sender: chan.Chan(^Accepted_Client, .Send),
	accepted_receiver: chan.Chan(^Accepted_Client, .Recv),
	response_sender: chan.Chan(^Response_Item, .Send),
	response_receiver: chan.Chan(^Response_Item, .Recv),
	wake_read_socket: net.TCP_Socket,
	wake_write_socket: net.TCP_Socket,
	wake_pending: bool,
	wake_buf: [64]byte,
	metrics_enabled: bool,
	metrics_interval: time.Duration,
	metrics_last_tick: time.Tick,
	metrics: IO_Metrics,
	ready_sender: chan.Chan(^IO_Thread_State, .Send),
}

IO_Metrics :: struct {
	total_ticks: int,
	empty_ticks: int,
	quiesce_rounds: int,
	wake_events: int,
	accepted_drained: int,
	responses_drained: int,
	requests_dispatched: int,
	recv_events: int,
	send_events: int,
	responses_sent: int,
}

Accepted_Client :: struct {
	id: u64,
	socket: net.TCP_Socket,
	source: net.Endpoint,
	work_sender: chan.Chan(^Work_Item, .Send),
	io_thread: ^IO_Thread_State,
}

Connection :: struct {
	id:            u64,
	socket: net.TCP_Socket,
	source: net.Endpoint,
	loop: ^nbio.Event_Loop,
	io_thread: ^IO_Thread_State,
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

io_tick_timeout_from_us :: proc(timeout_us: int) -> time.Duration {
	if timeout_us < 0 {
		return nbio.NO_TIMEOUT
	}
	return time.Duration(timeout_us) * time.Microsecond
}

create_io_wake_pair :: proc() -> (read_socket, write_socket: net.TCP_Socket, ok: bool) {
	fds: [2]posix.FD
	if posix.socketpair(.UNIX, .STREAM, .IP, &fds) != .OK {
		return
	}

	read_socket = net.TCP_Socket(fds[0])
	write_socket = net.TCP_Socket(fds[1])

	if err := net.set_blocking(read_socket, false); err != nil {
		net.close(read_socket)
		net.close(write_socket)
		read_socket = {}
		write_socket = {}
		return
	}
	if err := net.set_blocking(write_socket, false); err != nil {
		net.close(read_socket)
		net.close(write_socket)
		read_socket = {}
		write_socket = {}
		return
	}

	ok = true
	return
}

main :: proc() {
	context.logger = log.create_console_logger()
	options := Server_Options {
		workers            = DEFAULT_WORKER_COUNT,
		io_threads         = DEFAULT_IO_THREAD_COUNT,
		queue_size         = DEFAULT_WORK_QUEUE_SIZE,
		io_tick_timeout_us = DEFAULT_IO_TICK_TIMEOUT_US,
		io_quiesce_rounds  = DEFAULT_IO_QUIESCE_ROUNDS,
		metrics_ms         = DEFAULT_METRICS_INTERVAL_MS,
		profile_path       = DEFAULT_PROFILE_PATH,
	}
	flags.parse_or_exit(&options, os.args, .Unix)

	if options.workers <= 0 || options.io_threads <= 0 || options.queue_size <= 0 || options.metrics_ms <= 0 {
		log.panic("workers, io-threads, queue-size, and metrics-ms must be greater than zero")
	}
	if options.io_tick_timeout_us < -1 {
		log.panic("io-tick-timeout-us must be -1 or greater")
	}
	if options.io_quiesce_rounds < 0 {
		log.panic("io-quiesce-rounds must be zero or greater")
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

	work_channels := make([]chan.Chan(^Work_Item), options.io_threads)
	accepted_channels := make([]chan.Chan(^Accepted_Client), options.io_threads)
	response_channels := make([]chan.Chan(^Response_Item), options.io_threads)
	wake_read_sockets := make([]net.TCP_Socket, options.io_threads)
	wake_write_sockets := make([]net.TCP_Socket, options.io_threads)
	work_queues := make([]Work_Queue_State, options.io_threads)
	for i in 0 ..< options.io_threads {
		work_queue, queue_err := chan.create_buffered(chan.Chan(^Work_Item), options.queue_size, context.allocator)
		if queue_err != .None {
			log.panic("work queue create failed:", i, queue_err)
		}

		work_channels[i] = work_queue
		work_queues[i] = Work_Queue_State {
			index = i,
			sender = chan.as_send(work_queue),
			receiver = chan.as_recv(work_queue),
		}

		accepted_queue, accepted_queue_err := chan.create_buffered(chan.Chan(^Accepted_Client), options.queue_size, context.allocator)
		if accepted_queue_err != .None {
			log.panic("accepted queue create failed:", i, accepted_queue_err)
		}

		accepted_channels[i] = accepted_queue

		response_queue, response_queue_err := chan.create_buffered(chan.Chan(^Response_Item), options.queue_size, context.allocator)
		if response_queue_err != .None {
			log.panic("response queue create failed:", i, response_queue_err)
		}

		response_channels[i] = response_queue

		wake_read_socket, wake_write_socket, wake_ok := create_io_wake_pair()
		if !wake_ok {
			log.panic("io wake pair create failed:", i)
		}
		wake_read_sockets[i] = wake_read_socket
		wake_write_sockets[i] = wake_write_socket
	}
	defer {
		for work_queue in work_channels {
			chan.destroy(work_queue)
		}
		for accepted_queue in accepted_channels {
			chan.destroy(accepted_queue)
		}
		for response_queue in response_channels {
			chan.destroy(response_queue)
		}
		for wake_socket in wake_read_sockets {
			net.close(wake_socket)
		}
		for wake_socket in wake_write_sockets {
			net.close(wake_socket)
		}
		delete(work_channels)
		delete(accepted_channels)
		delete(response_channels)
		delete(wake_read_sockets)
		delete(wake_write_sockets)
		delete(work_queues)
	}

	worker_states := make([]Worker_State, options.workers)
	defer delete(worker_states)

	for i in 0 ..< options.workers {
		worker_states[i] = Worker_State {
			index = i,
			home_queue = i % len(work_queues),
			work_queues = work_queues,
			steal_work = options.steal_work,
		}

		t := thread.create_and_start_with_poly_data(
			&worker_states[i],
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
	tick_timeout := io_tick_timeout_from_us(options.io_tick_timeout_us)
	metrics_interval := time.Duration(options.metrics_ms) * time.Millisecond
	for i in 0 ..< options.io_threads {
		io_threads[i] = IO_Thread_State {
			index = i,
			tick_timeout = tick_timeout,
			quiesce_rounds = options.io_quiesce_rounds,
			work_sender = work_queues[i].sender,
			accepted_sender = chan.as_send(accepted_channels[i]),
			accepted_receiver = chan.as_recv(accepted_channels[i]),
			response_sender = chan.as_send(response_channels[i]),
			response_receiver = chan.as_recv(response_channels[i]),
			wake_read_socket = wake_read_sockets[i],
			wake_write_socket = wake_write_sockets[i],
			metrics_enabled = options.metrics,
			metrics_interval = metrics_interval,
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
		"queue_shards",
		len(work_queues),
		"io_tick_timeout_us",
		options.io_tick_timeout_us,
		"io_quiesce_rounds",
		options.io_quiesce_rounds,
		"steal_work",
		options.steal_work,
	)

	state := Server_State {
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
	if err := nbio.associate_socket(state.wake_read_socket, l=state.loop); err != nil {
		log.panic("io wake socket association failed:", state.index, err)
	}
	schedule_wake_recv(state)
	if err := nbio.tick(0); err != nil {
		log.panic("io event loop startup tick failed:", state.index, err)
	}
	if !chan.send(state.ready_sender, state) {
		log.panic("io thread ready signal failed:", state.index)
	}

	log.info("io thread started:", state.index)
	state.metrics_last_tick = time.tick_now()
	for {
		tick_io_thread(state, state.tick_timeout)
		drain_io_to_quiescence(state)
		maybe_log_io_metrics(state)
	}
}

tick_io_thread :: proc(io_thread: ^IO_Thread_State, timeout: time.Duration) -> bool {
	before_progress := io_metrics_progress(io_thread)
	drain_io_thread_queues(io_thread)
	when SPALL_PROFILE {
		profile_begin("io.tick")
	}
	err := nbio.tick(timeout)
	when SPALL_PROFILE {
		profile_end()
	}
	if err != nil {
		log.panic("io event loop tick failed:", io_thread.index, err)
	}
	drain_io_thread_queues(io_thread)
	after_progress := io_metrics_progress(io_thread)
	io_thread.metrics.total_ticks += 1
	if after_progress == before_progress {
		io_thread.metrics.empty_ticks += 1
		return false
	}
	return true
}

drain_io_to_quiescence :: proc(io_thread: ^IO_Thread_State) {
	for _ in 0 ..< io_thread.quiesce_rounds {
		if !tick_io_thread(io_thread, 0) {
			return
		}
		io_thread.metrics.quiesce_rounds += 1
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
	accepted.work_sender = io_thread.work_sender
	accepted.io_thread = io_thread

	if !chan.try_send(io_thread.accepted_sender, accepted) {
		net.close(accepted.socket)
		free(accepted)
		return
	}
	signal_io_thread(io_thread)
}

drain_io_thread_queues :: proc(io_thread: ^IO_Thread_State) {
	drain_accepted_clients(io_thread)
	drain_responses(io_thread)
}

schedule_wake_recv :: proc(io_thread: ^IO_Thread_State) {
	nbio.recv_poly(io_thread.wake_read_socket, {io_thread.wake_buf[:]}, io_thread, on_io_wake, l=io_thread.loop)
}

on_io_wake :: proc(op: ^nbio.Operation, io_thread: ^IO_Thread_State) {
	when SPALL_PROFILE {
		profile_begin("io.wake")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	if io_thread == nil {
		return
	}
	if op.recv.err != nil || op.recv.received == 0 {
		log.warn("io wake recv failed:", io_thread.index, op.recv.err)
		return
	}

	io_thread.metrics.wake_events += 1
	sync.atomic_store(&io_thread.wake_pending, false)
	drain_io_thread_queues(io_thread)
	schedule_wake_recv(io_thread)
}

signal_io_thread :: proc(io_thread: ^IO_Thread_State) {
	if io_thread == nil {
		return
	}

	if sync.atomic_exchange(&io_thread.wake_pending, true) {
		return
	}

	wake_byte := [?]byte{1}
	_, err := net.send_tcp(io_thread.wake_write_socket, wake_byte[:])
	if err != nil {
		sync.atomic_store(&io_thread.wake_pending, false)
	}
}

drain_accepted_clients :: proc(io_thread: ^IO_Thread_State) {
	for {
		accepted, ok := chan.try_recv(io_thread.accepted_receiver)
		if !ok {
			return
		}

		io_thread.metrics.accepted_drained += 1
		assign_client_to_io_thread(io_thread, accepted)
	}
}

assign_client_to_io_thread :: proc(io_thread: ^IO_Thread_State, accepted: ^Accepted_Client) {
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

	if err := nbio.associate_socket(accepted.socket, l=io_thread.loop); err != nil {
		log.warn("client socket association failed:", err)
		net.close(accepted.socket)
		return
	}

	connection := new(Connection)
	connection.id = accepted.id
	connection.socket = accepted.socket
	connection.source = accepted.source
	connection.loop = io_thread.loop
	connection.io_thread = accepted.io_thread
	connection.work_sender = accepted.work_sender
	connection.pending_lines = make([dynamic][]byte)

	log.info("client connected:", connection.source)
	schedule_recv(connection)
}

work_worker :: proc(worker: ^Worker_State) {
	when SPALL_PROFILE {
		_ = profile_thread_start("request worker")
	}

	log.info("request worker started:", worker.index, "home_queue", worker.home_queue)
	for {
		when SPALL_PROFILE {
			profile_begin("worker.recv_wait")
		}
		work, ok := wait_for_work(worker)
		when SPALL_PROFILE {
			profile_end()
		}
		if !ok {
			continue
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

		enqueue_response(response)
	}
}

wait_for_work :: proc(worker: ^Worker_State) -> (work: ^Work_Item, ok: bool) {
	if worker.steal_work {
		return steal_or_wait_for_work(worker)
	}

	return chan.recv(worker.work_queues[worker.home_queue].receiver)
}

steal_or_wait_for_work :: proc(worker: ^Worker_State) -> (work: ^Work_Item, ok: bool) {
	work, ok = try_recv_from_queue(&worker.work_queues[worker.home_queue])
	if ok {
		return
	}

	when SPALL_PROFILE {
		profile_begin("worker.steal_scan")
	}
	for offset in 1 ..< len(worker.work_queues) {
		queue_index := (worker.home_queue + offset) % len(worker.work_queues)
		work, ok = try_recv_from_queue(&worker.work_queues[queue_index])
		if ok {
			when SPALL_PROFILE {
				profile_end()
			}
			return
		}
	}
	when SPALL_PROFILE {
		profile_end()
	}

	time.sleep(WORKER_IDLE_SLEEP)
	return nil, false
}

try_recv_from_queue :: proc(work_queue: ^Work_Queue_State) -> (work: ^Work_Item, ok: bool) {
	work, ok = chan.try_recv(work_queue.receiver)
	return
}

enqueue_response :: proc(response: ^Response_Item) {
	connection := response.connection
	if connection == nil || connection.io_thread == nil {
		delete(response.data)
		free(response)
		return
	}

	io_thread := connection.io_thread
	if !chan.send(io_thread.response_sender, response) {
		delete(response.data)
		free(response)
		return
	}
	signal_io_thread(io_thread)
}

drain_responses :: proc(io_thread: ^IO_Thread_State) {
	for {
		response, ok := chan.try_recv(io_thread.response_receiver)
		if !ok {
			return
		}

		io_thread.metrics.responses_drained += 1
		on_response_ready(response)
	}
}

io_metrics_progress :: proc(io_thread: ^IO_Thread_State) -> int {
	m := &io_thread.metrics
	return m.wake_events +
	       m.accepted_drained +
	       m.responses_drained +
	       m.requests_dispatched +
	       m.recv_events +
	       m.send_events +
	       m.responses_sent
}

maybe_log_io_metrics :: proc(io_thread: ^IO_Thread_State) {
	if !io_thread.metrics_enabled {
		return
	}
	if time.tick_since(io_thread.metrics_last_tick) < io_thread.metrics_interval {
		return
	}

	m := &io_thread.metrics
	useful_ticks := m.total_ticks - m.empty_ticks
	useful_pct := 0.0
	requests_per_tick := 0.0
	responses_per_tick := 0.0
	if m.total_ticks > 0 {
		useful_pct = f64(useful_ticks) * 100 / f64(m.total_ticks)
		requests_per_tick = f64(m.requests_dispatched) / f64(m.total_ticks)
		responses_per_tick = f64(m.responses_sent) / f64(m.total_ticks)
	}

	log.info(
		"io metrics",
		"index",
		io_thread.index,
		"ticks",
		m.total_ticks,
		"useful_ticks",
		useful_ticks,
		"empty_ticks",
		m.empty_ticks,
		"quiesce_rounds",
		m.quiesce_rounds,
		"useful_pct",
		useful_pct,
		"requests_per_tick",
		requests_per_tick,
		"responses_per_tick",
		responses_per_tick,
		"wake_events",
		m.wake_events,
		"accepted_drained",
		m.accepted_drained,
		"responses_drained",
		m.responses_drained,
		"requests_dispatched",
		m.requests_dispatched,
		"recv_events",
		m.recv_events,
		"send_events",
		m.send_events,
		"responses_sent",
		m.responses_sent,
	)
	io_thread.metrics_last_tick = time.tick_now()
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
	connection.io_thread.metrics.recv_events += 1

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
	connection.io_thread.metrics.requests_dispatched += 1
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

on_response_ready :: proc(response: ^Response_Item) {
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
	connection.io_thread.metrics.send_events += 1

	if op.send.err != nil {
		close_connection(connection)
		return
	}

	connection.io_thread.metrics.responses_sent += 1
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
