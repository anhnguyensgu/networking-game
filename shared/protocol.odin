package shared

import json "core:encoding/json"
import "core:net"

SERVER_PORT :: 43120
SERVER_ADDRESS :: "127.0.0.1:43120"
MAX_LINE_BYTES :: 128 * 1024

JSON_OPTIONS :: json.Marshal_Options {
	spec = .JSON,
	use_enum_names = true,
}

Message_Kind :: enum {
	Unknown,
	Get_World_Map,
	World_Map,
	Select_Base,
	Move_To,
	Aim,
	Shoot,
	Use_Item,
	Buy,
	World_Snapshot,
	Error,
}

Player_ID :: u64

Command_Kind :: enum {
	Unknown,
	System_Connected,
	System_Disconnected,
	Move_To,
	Aim,
	Shoot,
	Use_Item,
	Buy,
}

Envelope :: struct {
	kind: Message_Kind `json:"kind"`,
	seq:  u64          `json:"seq"`,
}

Get_World_Map_Request :: struct {
	kind: Message_Kind `json:"kind"`,
	seq:  u64          `json:"seq"`,
}

Select_Base_Request :: struct {
	kind:    Message_Kind `json:"kind"`,
	seq:     u64          `json:"seq"`,
	base_id: int          `json:"base_id"`,
}

Move_To_Request :: struct {
	kind:       Message_Kind `json:"kind"`,
	seq:        u64          `json:"seq"`,
	client_seq: u32          `json:"client_seq"`,
	x:          f32          `json:"x"`,
	y:          f32          `json:"y"`,
}

Aim_Request :: struct {
	kind:       Message_Kind `json:"kind"`,
	seq:        u64          `json:"seq"`,
	client_seq: u32          `json:"client_seq"`,
	angle:      f32          `json:"angle"`,
}

Shoot_Request :: struct {
	kind:             Message_Kind `json:"kind"`,
	seq:              u64          `json:"seq"`,
	client_seq:       u32          `json:"client_seq"`,
	target_player_id: Player_ID    `json:"target_player_id"`,
}

Use_Item_Request :: struct {
	kind:       Message_Kind `json:"kind"`,
	seq:        u64          `json:"seq"`,
	client_seq: u32          `json:"client_seq"`,
	item_id:    int          `json:"item_id"`,
}

Buy_Request :: struct {
	kind:       Message_Kind `json:"kind"`,
	seq:        u64          `json:"seq"`,
	client_seq: u32          `json:"client_seq"`,
	product_id: int          `json:"product_id"`,
}

Enemy_Base_View :: struct {
	id:    int    `json:"id"`,
	x:     f32    `json:"x"`,
	y:     f32    `json:"y"`,
	level: int    `json:"level"`,
	name:  string `json:"name"`,
}

World_Map_Response :: struct {
	kind:  Message_Kind     `json:"kind"`,
	seq:   u64              `json:"seq"`,
	bases: []Enemy_Base_View `json:"bases"`,
}

Player_Snapshot :: struct {
	player_id:     Player_ID `json:"player_id"`,
	connection_id: u64       `json:"connection_id"`,
	x:             f32       `json:"x"`,
	y:             f32       `json:"y"`,
	aim_angle:     f32       `json:"aim_angle"`,
}

World_Snapshot_Response :: struct {
	kind:        Message_Kind      `json:"kind"`,
	seq:         u64               `json:"seq"`,
	server_tick: u64               `json:"server_tick"`,
	players:     []Player_Snapshot `json:"players"`,
}

Error_Response :: struct {
	kind:    Message_Kind `json:"kind"`,
	seq:     u64          `json:"seq"`,
	message: string       `json:"message"`,
}

make_get_world_map_request :: proc(seq: u64) -> Get_World_Map_Request {
	return {kind = .Get_World_Map, seq = seq}
}

make_move_to_request :: proc(seq: u64, client_seq: u32, x, y: f32) -> Move_To_Request {
	return {kind = .Move_To, seq = seq, client_seq = client_seq, x = x, y = y}
}

make_aim_request :: proc(seq: u64, client_seq: u32, angle: f32) -> Aim_Request {
	return {kind = .Aim, seq = seq, client_seq = client_seq, angle = angle}
}

make_shoot_request :: proc(seq: u64, client_seq: u32, target_player_id: Player_ID) -> Shoot_Request {
	return {kind = .Shoot, seq = seq, client_seq = client_seq, target_player_id = target_player_id}
}

make_use_item_request :: proc(seq: u64, client_seq: u32, item_id: int) -> Use_Item_Request {
	return {kind = .Use_Item, seq = seq, client_seq = client_seq, item_id = item_id}
}

make_buy_request :: proc(seq: u64, client_seq: u32, product_id: int) -> Buy_Request {
	return {kind = .Buy, seq = seq, client_seq = client_seq, product_id = product_id}
}

make_world_map_response :: proc(seq: u64, bases: []Enemy_Base_View) -> World_Map_Response {
	return {kind = .World_Map, seq = seq, bases = bases}
}

make_world_snapshot_response :: proc(seq, server_tick: u64, players: []Player_Snapshot) -> World_Snapshot_Response {
	return {kind = .World_Snapshot, seq = seq, server_tick = server_tick, players = players}
}

make_error_response :: proc(seq: u64, message: string) -> Error_Response {
	return {kind = .Error, seq = seq, message = message}
}

destroy_enemy_base_views :: proc(bases: []Enemy_Base_View) {
	for base in bases {
		delete(base.name)
	}
	delete(bases)
}

destroy_world_map_response :: proc(response: ^World_Map_Response) {
	destroy_enemy_base_views(response.bases)
	response.bases = nil
}

encode_json :: proc(message: any, allocator := context.allocator) -> (data: []byte, err: json.Marshal_Error) {
	return json.marshal(message, JSON_OPTIONS, allocator)
}

decode_json :: proc(data: []byte, out: any, allocator := context.allocator) -> json.Unmarshal_Error {
	return json.unmarshal_any(data, out, allocator = allocator)
}

decode_envelope :: proc(data: []byte, allocator := context.allocator) -> (envelope: Envelope, err: json.Unmarshal_Error) {
	err = decode_json(data, &envelope, allocator)
	return
}

send_json_line :: proc(socket: net.TCP_Socket, message: any) -> bool {
	data, err := encode_json(message)
	if err != nil {
		return false
	}
	defer delete(data)

	if _, send_err := net.send_tcp(socket, data); send_err != nil {
		return false
	}
	if _, send_err := net.send_tcp(socket, transmute([]byte)string("\n")); send_err != nil {
		return false
	}
	return true
}

read_json_line :: proc(socket: net.TCP_Socket, out: []byte) -> (line: []byte, ok: bool) {
	line_len := 0
	buf: [512]byte

	for line_len < len(out) {
		n, err := net.recv_tcp(socket, buf[:])
		if err != nil || n == 0 {
			return nil, false
		}

		for b in buf[:n] {
			if b == '\n' {
				return out[:line_len], true
			}
			if line_len >= len(out) {
				return nil, false
			}
			out[line_len] = b
			line_len += 1
		}
	}

	return nil, false
}
