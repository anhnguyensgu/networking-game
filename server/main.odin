package main

import shared "../shared"
import "core:log"
import "core:net"

ENEMY_BASES := []shared.Enemy_Base_View {
	{id = 1, x = 180, y = 160, level = 2, name = "Stone Reef"},
	{id = 2, x = 420, y = 260, level = 4, name = "Iron Cove"},
	{id = 3, x = 660, y = 180, level = 6, name = "Storm Pier"},
	{id = 4, x = 540, y = 420, level = 8, name = "Crab Harbor"},
}

main :: proc() {
	context.logger = log.create_console_logger()
	endpoint := net.Endpoint {
		address = net.IP4_Loopback,
		port    = shared.SERVER_PORT,
	}
	listener, listen_err := net.listen_tcp(endpoint)
	if listen_err != nil {
		log.panic("server listen failed", listen_err)
	}
	defer net.close(listener)

	log.info("server listening on", shared.SERVER_ADDRESS)

	for {
		client, source, accept_err := net.accept_tcp(listener)
		if accept_err != nil {
			log.warn("accept failed:", accept_err)
			continue
		}

		handle_client(client, source)
	}
}

handle_client :: proc(client: net.TCP_Socket, source: net.Endpoint) {
	log.info("client connected:", source)
	line_buf: [shared.MAX_LINE_BYTES]byte
	defer {
		net.close(client)
		log.info("client disconnected")
	}

	for {
		line, ok := shared.read_json_line(client, line_buf[:])
		if !ok {
			return
		}

		envelope, err := shared.decode_envelope(line)
		if err != nil {
			_ = shared.send_json_line(
				client,
				shared.make_error_response(0, "invalid JSON message"),
			)
			continue
		}

		#partial switch envelope.kind {
		case .Get_World_Map:
			request: shared.Get_World_Map_Request
			if err := shared.decode_json(line, &request); err != nil {
				_ = shared.send_json_line(
					client,
					shared.make_error_response(envelope.seq, "invalid world map request"),
				)
				continue
			}
			response := shared.make_world_map_response(request.seq, ENEMY_BASES)
			_ = shared.send_json_line(client, response)

		case .Select_Base:
			request: shared.Select_Base_Request
			if err := shared.decode_json(line, &request); err != nil {
				_ = shared.send_json_line(
					client,
					shared.make_error_response(envelope.seq, "invalid select base request"),
				)
				continue
			}
			_ = shared.send_json_line(
				client,
				shared.make_error_response(request.seq, "battle screen is not implemented yet"),
			)

		case:
			_ = shared.send_json_line(
				client,
				shared.make_error_response(envelope.seq, "unknown message kind"),
			)
		}
	}

}
