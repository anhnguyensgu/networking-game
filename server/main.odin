package main

import shared "../shared"
import "core:flags"
import "core:log"
import "core:nbio"
import "core:net"
import "core:os"
import "core:sync/chan"
import "core:thread"

DEFAULT_WORKER_COUNT :: 14
DEFAULT_WORK_QUEUE_SIZE :: 256
RECV_BUFFER_SIZE :: 4096

Server_Options :: struct {
	workers:    int `usage:"Number of request worker threads."`,
	queue_size: int `args:"name=queue-size" usage:"Request work queue capacity."`,
}

Server_State :: struct {
	work_sender: chan.Chan(^Work_Item, .Send),
	next_conn_id: u64,
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

ENEMY_BASES := []shared.Enemy_Base_View {
	{id = 1, x = 180, y = 160, level = 2, name = "Stone Reef"},
	{id = 2, x = 420, y = 260, level = 4, name = "Iron Cove"},
	{id = 3, x = 660, y = 180, level = 6, name = "Storm Pier"},
	{id = 4, x = 540, y = 420, level = 8, name = "Crab Harbor"},
}

main :: proc() {
	context.logger = log.create_console_logger()
	options := Server_Options {
		workers    = DEFAULT_WORKER_COUNT,
		queue_size = DEFAULT_WORK_QUEUE_SIZE,
	}
	flags.parse_or_exit(&options, os.args, .Unix)

	if options.workers <= 0 || options.queue_size <= 0 {
		log.panic("workers and queue-size must be greater than zero")
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

	log.info("server listening on", shared.SERVER_ADDRESS, "workers", options.workers, "queue_size", options.queue_size)

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

	state := Server_State {
		work_sender = work_sender,
		next_conn_id = 1,
	}
	nbio.accept_poly(listener, &state, on_accept)

	for {
		if err := nbio.tick(); err != nil {
			log.panic("event loop tick failed:", err)
		}
	}
}

on_accept :: proc(op: ^nbio.Operation, state: ^Server_State) {
	if op.accept.err != nil {
		log.warn("accept failed:", op.accept.err)
		nbio.accept_poly(op.accept.socket, state, on_accept)
		return
	}

	nbio.accept_poly(op.accept.socket, state, on_accept)

	connection := new(Connection)
	connection.id = state.next_conn_id
	state.next_conn_id += 1
	connection.socket = op.accept.client
	connection.source = op.accept.client_endpoint
	connection.loop = op.l
	connection.work_sender = state.work_sender
	connection.pending_lines = make([dynamic][]byte)

	log.info("client connected:", connection.source)
	schedule_recv(connection)
}

work_worker :: proc(index: int, jobs: chan.Chan(^Work_Item, .Recv)) {
	log.info("request worker started:", index)
	for {
		work, ok := chan.recv(jobs)
		if !ok {
			return
		}

		response := new(Response_Item)
		response.connection = work.connection
		response.data = process_request(work.line)

		delete(work.line)
		free(work)

		nbio.next_tick_poly(response, on_response_ready, l=response.connection.loop)
	}
}

schedule_recv :: proc(connection: ^Connection) {
	if connection.closed || connection.busy {
		return
	}
	nbio.recv_poly(connection.socket, {connection.recv_buf[:]}, connection, on_recv, l=connection.loop)
}

on_recv :: proc(op: ^nbio.Operation, connection: ^Connection) {
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
	if connection.closed || connection.busy || len(connection.pending_lines) == 0 {
		return
	}

	line := pop_front(&connection.pending_lines)
	work := new(Work_Item)
	work.connection = connection
	work.line = line
	connection.busy = true

	if !chan.try_send(connection.work_sender, work) {
		delete(work.line)
		free(work)
		connection.busy = false
		close_connection(connection)
	}
}

process_request :: proc(line: []byte) -> []byte {
	envelope, err := shared.decode_envelope(line)
	if err != nil {
		return encode_json_line(shared.make_error_response(0, "invalid JSON message"))
	}

	#partial switch envelope.kind {
	case .Get_World_Map:
		request: shared.Get_World_Map_Request
		if err := shared.decode_json(line, &request); err != nil {
			return encode_json_line(shared.make_error_response(envelope.seq, "invalid world map request"))
		}
		return encode_json_line(shared.make_world_map_response(request.seq, ENEMY_BASES))

	case .Select_Base:
		request: shared.Select_Base_Request
		if err := shared.decode_json(line, &request); err != nil {
			return encode_json_line(shared.make_error_response(envelope.seq, "invalid select base request"))
		}
		return encode_json_line(shared.make_error_response(request.seq, "battle screen is not implemented yet"))

	case:
		return encode_json_line(shared.make_error_response(envelope.seq, "unknown message kind"))
	}
}

encode_json_line :: proc(message: any) -> []byte {
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
