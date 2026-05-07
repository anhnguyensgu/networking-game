package main

import "core:c"
import "core:fmt"
import "core:log"
import "core:math"
import "core:net"
import "core:strings"

import rl "vendor:raylib"

import shared "../shared"

Client_State :: struct {
	status:   Status,
	bases:    []shared.Enemy_Base_View,
	selected: int,
}

Status :: union {
	Status_Connecting,
	Status_Connected,
	Status_Failed,
}

Status_Connecting :: struct {}
Status_Connected :: struct {
	base_count: int,
}
Status_Failed :: struct {
	message: string,
}

OFFLINE_SERVER_MESSAGE :: "offline: start server at " + shared.SERVER_ADDRESS

main :: proc() {
	context.logger = log.create_console_logger()
	defer log.destroy_console_logger(context.logger)

	state := Client_State {
		status   = Status_Connecting{},
		selected = -1,
	}
	defer shared.destroy_enemy_base_views(state.bases)

	log.info("connecting to server:", shared.SERVER_ADDRESS)
	socket, dial_err := net.dial_tcp(shared.SERVER_ADDRESS)
	if dial_err != nil {
		//just retry later
		log.panic("failed to connect to server:", dial_err)
	}
	log.info("connected to server:", shared.SERVER_ADDRESS)

	load_world_map(socket, &state)

	rl.InitWindow(960, 640, cstring("Odin Island Map"))
	defer rl.CloseWindow()
	rl.SetTargetFPS(60)

	for !rl.WindowShouldClose() {
		update_selection(&state)

		rl.BeginDrawing()
		rl.ClearBackground(rl.Color{36, 92, 112, 255})
		draw_world_map(&state)
		rl.EndDrawing()
	}
}

load_world_map :: proc(socket: net.TCP_Socket, state: ^Client_State) {
	request := shared.make_get_world_map_request(1)
	if !shared.send_json_line(socket, request) {
		state.status = Status_Failed {
			message = "failed to send world map request",
		}
		return
	}

	line_buf: [shared.MAX_LINE_BYTES]byte
	line, ok := shared.read_json_line(socket, line_buf[:])
	if !ok {
		state.status = Status_Failed {
			message = "server closed before world map response",
		}
		return
	}

	envelope, envelope_err := shared.decode_envelope(line)
	if envelope_err != nil {
		state.status = Status_Failed {
			message = "server returned invalid JSON",
		}
		return
	}

	#partial switch envelope.kind {
	case .World_Map:
		response: shared.World_Map_Response
		if err := shared.decode_json(line, &response); err != nil {
			state.status = Status_Failed {
				message = "failed to decode world map response",
			}
			return
		}

		shared.destroy_enemy_base_views(state.bases)
		state.bases = response.bases
		state.status = Status_Connected {
			base_count = len(state.bases),
		}

	case .Error:
		response: shared.Error_Response
		if err := shared.decode_json(line, &response); err == nil {
			delete(response.message)
		}
		state.status = Status_Failed {
			message = "server returned an error",
		}

	case:
		state.status = Status_Failed {
			message = "unexpected server response",
		}
	}
}

update_selection :: proc(state: ^Client_State) {
	if !rl.IsMouseButtonPressed(.LEFT) {
		return
	}

	mouse := rl.GetMousePosition()
	state.selected = -1

	for base in state.bases {
		dx := mouse.x - base.x
		dy := mouse.y - base.y
		if math.sqrt(dx * dx + dy * dy) <= 30 {
			state.selected = base.id
			return
		}
	}
}

draw_world_map :: proc(state: ^Client_State) {
	draw_ocean_grid()
	draw_header(state)

	for base in state.bases {
		selected := base.id == state.selected
		draw_base_node(base, selected)
	}

	if state.selected >= 0 {
		draw_selection_panel(state)
	}
}

draw_ocean_grid :: proc() {
	for x := 0; x < 960; x += 80 {
		rl.DrawRectangle(c.int(x), 0, 1, 640, rl.Color{255, 255, 255, 18})
	}
	for y := 0; y < 640; y += 80 {
		rl.DrawRectangle(0, c.int(y), 960, 1, rl.Color{255, 255, 255, 18})
	}

	rl.DrawCircle(180, 160, 78, rl.Color{79, 151, 101, 255})
	rl.DrawCircle(420, 260, 94, rl.Color{88, 164, 105, 255})
	rl.DrawCircle(660, 180, 84, rl.Color{84, 149, 94, 255})
	rl.DrawCircle(540, 420, 108, rl.Color{93, 159, 98, 255})
}

draw_header :: proc(state: ^Client_State) {
	rl.DrawRectangle(0, 0, 960, 58, rl.Color{20, 42, 51, 230})
	rl.DrawText(cstring("ODIN ISLAND MAP"), 24, 18, 24, rl.RAYWHITE)

	status_text := get_status_text(state.status)
	status_cstr :=
		strings.clone_to_cstring(status_text, context.temp_allocator) or_else cstring(
			"status unavailable",
		)
	rl.DrawText(status_cstr, 660, 22, 16, rl.Color{202, 233, 222, 255})
}

get_status_text :: proc(status: Status) -> string {
	switch s in status {
	case Status_Connecting:
		return "connecting to server..."
	case Status_Connected:
		return fmt.tprintf("connected: %d enemy bases", s.base_count)
	case Status_Failed:
		return s.message
	}

	return "status unavailable"
}

draw_base_node :: proc(base: shared.Enemy_Base_View, selected: bool) {
	color := rl.Color{206, 76, 54, 255}
	if selected {
		color = rl.Color{247, 203, 72, 255}
	}

	rl.DrawCircle(c.int(base.x), c.int(base.y), 24, color)
	rl.DrawCircleLines(c.int(base.x), c.int(base.y), 30, rl.RAYWHITE)

	label := fmt.tprintf("%s L%d", base.name, base.level)
	label_cstr := strings.clone_to_cstring(label, context.temp_allocator) or_else cstring("base")
	rl.DrawText(label_cstr, c.int(base.x) - 52, c.int(base.y) + 38, 16, rl.RAYWHITE)
}

draw_selection_panel :: proc(state: ^Client_State) {
	for base in state.bases {
		if base.id != state.selected {
			continue
		}

		rl.DrawRectangle(24, 500, 360, 104, rl.Color{18, 33, 39, 235})
		rl.DrawRectangleLines(24, 500, 360, 104, rl.RAYWHITE)

		title := fmt.tprintf("%s", base.name)
		title_cstr :=
			strings.clone_to_cstring(title, context.temp_allocator) or_else cstring("base")
		rl.DrawText(title_cstr, 44, 520, 24, rl.RAYWHITE)

		meta := fmt.tprintf("Enemy base level %d", base.level)
		meta_cstr :=
			strings.clone_to_cstring(meta, context.temp_allocator) or_else cstring("level")
		rl.DrawText(meta_cstr, 44, 556, 18, rl.Color{207, 232, 219, 255})

		rl.DrawText(cstring("Battle screen comes next."), 44, 580, 16, rl.Color{247, 203, 72, 255})
		return
	}
}
