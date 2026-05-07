package shared

import "core:testing"

@(test)
world_map_response_round_trips_json :: proc(t: ^testing.T) {
	bases := []Enemy_Base_View {
		{id = 1, x = 220, y = 140, level = 3, name = "Stone Reef"},
		{id = 2, x = 420, y = 260, level = 5, name = "Iron Cove"},
	}

	response := make_world_map_response(7, bases)
	data, err := encode_json(response)
	if !testing.expectf(t, err == nil, "encode_json failed: %v", err) {
		return
	}
	defer delete(data)

	decoded: World_Map_Response
	if err := decode_json(data, &decoded); !testing.expectf(t, err == nil, "decode_json failed: %v", err) {
		return
	}
	defer destroy_world_map_response(&decoded)

	testing.expect_value(t, decoded.kind, Message_Kind.World_Map)
	testing.expect_value(t, decoded.seq, u64(7))
	testing.expect_value(t, len(decoded.bases), 2)
	testing.expect_value(t, decoded.bases[0].name, "Stone Reef")
	testing.expect_value(t, decoded.bases[1].level, 5)
}

@(test)
envelope_decodes_message_kind :: proc(t: ^testing.T) {
	data := transmute([]byte)string(`{"kind":"Get_World_Map","seq":9}`)

	envelope, err := decode_envelope(data)
	if !testing.expectf(t, err == nil, "decode_envelope failed: %v", err) {
		return
	}

	testing.expect_value(t, envelope.kind, Message_Kind.Get_World_Map)
	testing.expect_value(t, envelope.seq, u64(9))
}
