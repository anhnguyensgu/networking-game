package main

import shared "../shared"
import json "core:encoding/json"
import "core:flags"
import "core:log"
import "core:math"
import "core:nbio"
import "core:net"
import "core:os"
import "core:prof/spall"
import "core:strings"
import "core:sync"
import "core:sync/chan"
import "core:thread"
import "core:time"

DEFAULT_WORKER_COUNT :: 14
DEFAULT_IO_THREAD_COUNT :: 1
DEFAULT_WORK_QUEUE_SIZE :: 8192
DEFAULT_GAME_COMMAND_QUEUE_SIZE :: 8192
DEFAULT_BROADCAST_QUEUE_SIZE :: 1024
RECV_BUFFER_SIZE :: 4096
WORKER_IDLE_SLEEP :: 50 * time.Microsecond
DEFAULT_IO_TICK_TIMEOUT_US :: -1
DEFAULT_IO_QUIESCE_ROUNDS :: 8
DEFAULT_METRICS_INTERVAL_MS :: 1000
DEFAULT_CPU_WORK_US :: 0
DEFAULT_PROFILE_PATH :: "profiles/server.spall"
PROFILE_BUFFER_SIZE :: 64 * 1024
PROFILE_FLUSH_EVENTS :: 512
CPU_WORK_BATCH_SIZE :: 256
INPUT_DELAY_TICKS :: u64(2)
MAX_STALE_TICKS :: u64(20)
MAX_TOTAL_COMMANDS_PER_TICK :: 1000
MAX_FUTURE_COMMANDS :: 8192
MAX_FUTURE_COMMANDS_PER_CONNECTION :: 8
BROADCAST_INTERVAL_TICKS :: u64(3)
SPALL_PROFILE :: #config(SPALL_PROFILE, false)

NPC_COUNT :: 100000

NPC :: struct {
	x, y: f32,
}

cpu_work_sink: u64
current_server_tick: u64
game_command_sender: chan.Chan(^Game_Command, .Send)

Server_Options :: struct {
	workers:      int    `usage:"Number of request worker threads."`,
	io_threads:   int    `args:"name=io-threads" usage:"Number of IO event-loop threads for accepted clients."`,
	queue_size:   int    `args:"name=queue-size" usage:"Request work queue capacity per IO shard."`,
	io_tick_timeout_us: int `args:"name=io-tick-timeout-us" usage:"IO loop tick timeout in microseconds. -1 blocks until socket or explicit wake."`,
	io_quiesce_rounds: int `args:"name=io-quiesce-rounds" usage:"Maximum non-blocking IO drain rounds after each blocking tick. Zero disables quiescing."`,
	cpu_work_us:  int    `args:"name=cpu-work-us" usage:"Approximate CPU work to burn per valid request in microseconds. Zero disables."`,
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
	cpu_work: time.Duration,
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
	broadcast_sender: chan.Chan(^Broadcast_Shard_Message, .Send),
	broadcast_receiver: chan.Chan(^Broadcast_Shard_Message, .Recv),
	connections: [dynamic]^Connection,
	task_drain_pending: bool,
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
	accepted_tasks: int,
	response_tasks: int,
	broadcast_tasks: int,
	broadcast_sends: int,
	requests_dispatched: int,
	recv_events: int,
	send_events: int,
	responses_sent: int,
}

IO_Tick_Result :: struct {
	progress: bool,
	queue_progress: bool,
	event_progress: bool,
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
	receives_snapshots: bool,
	closed: bool,
}

Work_Item :: struct {
	connection: ^Connection,
	line: []byte,
	is_pooled: bool,
}

Response_Item :: struct {
	connection: ^Connection,
	data: []byte,
	release_only: bool,
	is_pooled: bool,
}

Request_Result :: struct {
	data: []byte,
	release_only: bool,
}

Broadcast_Shard_Message :: struct {
	data: []byte,
	remaining_sends: int,
}

Game_Command :: struct {
	connection_id: u64,
	client_seq: u32,
	recv_tick: u64,
	target_tick: u64,
	kind: shared.Command_Kind,
	x: f32,
	y: f32,
	aim_angle: f32,
	target_player_id: shared.Player_ID,
	item_id: int,
	product_id: int,
	is_pooled: bool,
}

Work_Item_Pool: struct {
	items: [dynamic]^Work_Item,
	mutex: sync.Mutex,
}

Response_Item_Pool: struct {
	items: [dynamic]^Response_Item,
	mutex: sync.Mutex,
}

Game_Command_Pool: struct {
	items: [dynamic]^Game_Command,
	mutex: sync.Mutex,
}

Line_Buffer_Pool: struct {
	items: [dynamic][]byte,
	mutex: sync.Mutex,
}

alloc_work_item :: proc() -> ^Work_Item {
	sync.mutex_lock(&Work_Item_Pool.mutex)
	defer sync.mutex_unlock(&Work_Item_Pool.mutex)
	if len(Work_Item_Pool.items) > 0 {
		item := pop(&Work_Item_Pool.items)
		item^ = {}
		item.is_pooled = true
		return item
	}
	item := new(Work_Item)
	item.is_pooled = true
	return item
}

free_work_item :: proc(item: ^Work_Item) {
	if item == nil do return
	if !item.is_pooled {
		free(item)
		return
	}
	sync.mutex_lock(&Work_Item_Pool.mutex)
	defer sync.mutex_unlock(&Work_Item_Pool.mutex)
	append(&Work_Item_Pool.items, item)
}

alloc_response_item :: proc() -> ^Response_Item {
	sync.mutex_lock(&Response_Item_Pool.mutex)
	defer sync.mutex_unlock(&Response_Item_Pool.mutex)
	if len(Response_Item_Pool.items) > 0 {
		item := pop(&Response_Item_Pool.items)
		item^ = {}
		item.is_pooled = true
		return item
	}
	item := new(Response_Item)
	item.is_pooled = true
	return item
}

free_response_item :: proc(item: ^Response_Item) {
	if item == nil do return
	if !item.is_pooled {
		free(item)
		return
	}
	sync.mutex_lock(&Response_Item_Pool.mutex)
	defer sync.mutex_unlock(&Response_Item_Pool.mutex)
	append(&Response_Item_Pool.items, item)
}

alloc_game_command :: proc() -> ^Game_Command {
	sync.mutex_lock(&Game_Command_Pool.mutex)
	defer sync.mutex_unlock(&Game_Command_Pool.mutex)
	if len(Game_Command_Pool.items) > 0 {
		item := pop(&Game_Command_Pool.items)
		item^ = {}
		item.is_pooled = true
		return item
	}
	item := new(Game_Command)
	item.is_pooled = true
	return item
}

free_game_command :: proc(item: ^Game_Command) {
	if item == nil do return
	if !item.is_pooled {
		free(item)
		return
	}
	sync.mutex_lock(&Game_Command_Pool.mutex)
	defer sync.mutex_unlock(&Game_Command_Pool.mutex)
	append(&Game_Command_Pool.items, item)
}

alloc_line_buffer :: proc(size: int) -> []byte {
	sync.mutex_lock(&Line_Buffer_Pool.mutex)
	if len(Line_Buffer_Pool.items) > 0 {
		buf := pop(&Line_Buffer_Pool.items)
		sync.mutex_unlock(&Line_Buffer_Pool.mutex)
		return buf[:size]
	}
	sync.mutex_unlock(&Line_Buffer_Pool.mutex)
	buf := make([]byte, shared.MAX_LINE_BYTES)
	return buf[:size]
}

free_line_buffer :: proc(line: []byte) {
	if line == nil || raw_data(line) == nil do return
	// Recover the full MAX_LINE_BYTES backing regardless of the sub-slice length.
	full := ([^]byte)(raw_data(line))[:shared.MAX_LINE_BYTES]
	sync.mutex_lock(&Line_Buffer_Pool.mutex)
	defer sync.mutex_unlock(&Line_Buffer_Pool.mutex)
	append(&Line_Buffer_Pool.items, full)
}

Player :: struct {
	id: shared.Player_ID,
	connection_id: u64,
	x: f32,
	y: f32,
	aim_angle: f32,
	last_client_seq: u32,
	subscribed: bool,
}

World :: struct {
	next_player_id: shared.Player_ID,
	players: [dynamic]Player,
	npcs: []NPC,
}

Simulation_State :: struct {
	world: World,
	future_commands: [dynamic]Game_Command,
	ready_commands: [dynamic]Game_Command,
	gameplay_commands: [dynamic]Game_Command,
	keep_commands: [dynamic]bool,
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

main :: proc() {
	context.logger = log.create_console_logger()
	options := Server_Options {
		workers            = DEFAULT_WORKER_COUNT,
		io_threads         = DEFAULT_IO_THREAD_COUNT,
		queue_size         = DEFAULT_WORK_QUEUE_SIZE,
		io_tick_timeout_us = DEFAULT_IO_TICK_TIMEOUT_US,
		io_quiesce_rounds  = DEFAULT_IO_QUIESCE_ROUNDS,
		cpu_work_us        = DEFAULT_CPU_WORK_US,
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
	if options.cpu_work_us < 0 {
		log.panic("cpu-work-us must be zero or greater")
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
	broadcast_channels := make([]chan.Chan(^Broadcast_Shard_Message), options.io_threads)
	work_queues := make([]Work_Queue_State, options.io_threads)
	game_command_queue, command_queue_err := chan.create_buffered(chan.Chan(^Game_Command), DEFAULT_GAME_COMMAND_QUEUE_SIZE, context.allocator)
	if command_queue_err != .None {
		log.panic("game command queue create failed:", command_queue_err)
	}
	defer chan.destroy(game_command_queue)
	game_command_sender = chan.as_send(game_command_queue)
	game_command_receiver := chan.as_recv(game_command_queue)
	sync.atomic_store(&current_server_tick, u64(0))

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

		broadcast_queue, broadcast_queue_err := chan.create_buffered(chan.Chan(^Broadcast_Shard_Message), DEFAULT_BROADCAST_QUEUE_SIZE, context.allocator)
		if broadcast_queue_err != .None {
			log.panic("broadcast queue create failed:", i, broadcast_queue_err)
		}

		broadcast_channels[i] = broadcast_queue
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
		for broadcast_queue in broadcast_channels {
			chan.destroy(broadcast_queue)
		}
		delete(work_channels)
		delete(accepted_channels)
		delete(response_channels)
		delete(broadcast_channels)
		delete(work_queues)
	}

	worker_states := make([]Worker_State, options.workers)
	defer delete(worker_states)

	cpu_work := time.Duration(options.cpu_work_us) * time.Microsecond
	for i in 0 ..< options.workers {
		worker_states[i] = Worker_State {
			index = i,
			home_queue = i % len(work_queues),
			work_queues = work_queues,
			cpu_work = cpu_work,
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
			broadcast_sender = chan.as_send(broadcast_channels[i]),
			broadcast_receiver = chan.as_recv(broadcast_channels[i]),
			connections = make([dynamic]^Connection),
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
	defer {
		for i in 0 ..< len(io_threads) {
			delete(io_threads[i].connections)
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
		"cpu_work_us",
		options.cpu_work_us,
		"steal_work",
		options.steal_work,
	)

	state := Server_State {
		next_conn_id = 1,
		io_threads = io_threads,
	}
	nbio.accept_poly(listener, &state, on_accept)

	sim := Simulation_State {
		world = World {
			next_player_id = 1,
			players = make([dynamic]Player),
			npcs = make([]NPC, NPC_COUNT),
		},
		future_commands = make([dynamic]Game_Command),
		ready_commands = make([dynamic]Game_Command),
		gameplay_commands = make([dynamic]Game_Command),
		keep_commands = make([dynamic]bool),
	}
	for i in 0 ..< NPC_COUNT {
		sim.world.npcs[i] = NPC { x = f32(i % 1000), y = f32(i / 1000) }
	}
	defer destroy_simulation_state(&sim)
	server_tick: u64 = 0

	tick_rate: f64 = 60.0
	tick_duration: time.Duration = time.Duration(f64(time.Second) / tick_rate)
	
	stopwatch: time.Stopwatch
	time.stopwatch_start(&stopwatch)
	
	accumulator: time.Duration = 0
	last_time: time.Duration = time.stopwatch_duration(stopwatch)

	for {
		current_time: time.Duration = time.stopwatch_duration(stopwatch)
		dt: time.Duration = current_time - last_time
		last_time = current_time
		
		accumulator += dt
		
		simulated_this_frame := false
		for accumulator >= tick_duration {
			when SPALL_PROFILE {
				profile_begin("simulation.tick")
			}
			run_simulation_tick(&sim, game_command_receiver, io_threads, server_tick)
			when SPALL_PROFILE {
				profile_end()
			}
			
			accumulator -= tick_duration
			server_tick += 1
			sync.atomic_store(&current_server_tick, server_tick)
			simulated_this_frame = true
		}
		
		sleep_duration: time.Duration = 0
		if accumulator < tick_duration {
			sleep_duration = tick_duration - accumulator
		}

		when SPALL_PROFILE {
			profile_begin("accept.tick")
		}
		timeout := simulated_this_frame ? time.Duration(0) : sleep_duration
		err := nbio.tick(timeout)
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
	state.metrics_last_tick = time.tick_now()
	for {
		result := tick_io_thread(state, state.tick_timeout, prepare_to_wait=true)
		drain_io_to_quiescence(state, result)
		maybe_log_io_metrics(state)
	}
}

tick_io_thread :: proc(io_thread: ^IO_Thread_State, timeout: time.Duration, prepare_to_wait := false) -> IO_Tick_Result {
	before_queue_progress := io_queue_progress(io_thread)
	before_event_progress := io_event_progress(io_thread)
	drain_io_thread_queues(io_thread)
	if prepare_to_wait {
		sync.atomic_store(&io_thread.task_drain_pending, false)
		drain_io_thread_queues(io_thread)
	}
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
	after_queue_progress := io_queue_progress(io_thread)
	after_event_progress := io_event_progress(io_thread)

	result := IO_Tick_Result {
		queue_progress = after_queue_progress != before_queue_progress,
		event_progress = after_event_progress != before_event_progress,
	}
	result.progress = result.queue_progress || result.event_progress
	io_thread.metrics.total_ticks += 1
	if !result.progress {
		io_thread.metrics.empty_ticks += 1
	}
	return result
}

drain_io_to_quiescence :: proc(io_thread: ^IO_Thread_State, previous: IO_Tick_Result) {
	if !previous.progress {
		return
	}

	for _ in 0 ..< io_thread.quiesce_rounds {
		result := tick_io_thread(io_thread, 0)
		if !result.progress {
			return
		}
		io_thread.metrics.quiesce_rounds += 1
		if result.queue_progress && !result.event_progress {
			return
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
	accepted.work_sender = io_thread.work_sender
	accepted.io_thread = io_thread

	if io_thread.loop == nil {
		net.close(accepted.socket)
		free(accepted)
		return
	}
	if !chan.try_send(io_thread.accepted_sender, accepted) {
		net.close(accepted.socket)
		free(accepted)
		return
	}
	schedule_io_thread_task_drain(io_thread)
}

schedule_io_thread_task_drain :: proc(io_thread: ^IO_Thread_State) {
	if io_thread == nil || io_thread.loop == nil {
		return
	}
	if sync.atomic_exchange(&io_thread.task_drain_pending, true) {
		return
	}
	nbio.wake_up(io_thread.loop)
}

drain_io_thread_queues :: proc(io_thread: ^IO_Thread_State) {
	drain_accepted_clients(io_thread)
	drain_responses(io_thread)
	drain_broadcasts(io_thread)
}

drain_accepted_clients :: proc(io_thread: ^IO_Thread_State) {
	for {
		accepted, ok := chan.try_recv(io_thread.accepted_receiver)
		if !ok {
			return
		}

		io_thread.metrics.accepted_tasks += 1
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
	append(&io_thread.connections, connection)
	enqueue_system_command(connection.id, .System_Connected)

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
		response := alloc_response_item()
		response.connection = work.connection
		result := process_request(work.connection, work.line, worker.cpu_work)
		response.data = result.data
		response.release_only = result.release_only

		free_line_buffer(work.line)
		free_work_item(work)
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
		free_response_item(response)
		return
	}

	io_thread := connection.io_thread
	if io_thread.loop == nil {
		delete(response.data)
		free_response_item(response)
		return
	}
	if !chan.send(io_thread.response_sender, response) {
		delete(response.data)
		free_response_item(response)
		return
	}
	schedule_io_thread_task_drain(io_thread)
}

drain_responses :: proc(io_thread: ^IO_Thread_State) {
	for {
		response, ok := chan.try_recv(io_thread.response_receiver)
		if !ok {
			return
		}

		io_thread.metrics.response_tasks += 1
		on_response_ready(response)
	}
}

io_queue_progress :: proc(io_thread: ^IO_Thread_State) -> int {
	m := &io_thread.metrics
	return m.accepted_tasks +
	       m.response_tasks +
	       m.broadcast_tasks
}

io_event_progress :: proc(io_thread: ^IO_Thread_State) -> int {
	m := &io_thread.metrics
	return m.requests_dispatched +
	       m.recv_events +
	       m.send_events +
	       m.responses_sent +
	       m.broadcast_sends
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
		"accepted_tasks",
		m.accepted_tasks,
		"response_tasks",
		m.response_tasks,
		"broadcast_tasks",
		m.broadcast_tasks,
		"broadcast_sends",
		m.broadcast_sends,
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
			line := alloc_line_buffer(connection.line_len)
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
	work := alloc_work_item()
	work.connection = connection
	work.line = line

	if !chan.try_send(connection.work_sender, work) {
		free_work_item(work)
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

process_request :: proc(connection: ^Connection, line: []byte, cpu_work: time.Duration) -> Request_Result {
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
		return {data = encode_json_line(shared.make_error_response(0, "invalid JSON message"))}
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
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid world map request"))}
		}
		simulate_cpu_work(cpu_work)
		return {data = encode_json_line(shared.make_world_map_response(request.seq, ENEMY_BASES))}

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
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid select base request"))}
		}
		simulate_cpu_work(cpu_work)
		return {data = encode_json_line(shared.make_error_response(request.seq, "battle screen is not implemented yet"))}

	case .Move_To:
		request: shared.Move_To_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil || connection == nil {
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid move request"))}
		}
		simulate_cpu_work(cpu_work)
		command := make_gameplay_command(connection.id, request.client_seq, .Move_To)
		command.x = request.x
		command.y = request.y
		if !enqueue_game_command(command) {
			return {data = encode_json_line(shared.make_error_response(request.seq, "server command queue is full"))}
		}
		connection.receives_snapshots = true
		return {release_only = true}

	case .Aim:
		request: shared.Aim_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil || connection == nil {
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid aim request"))}
		}
		simulate_cpu_work(cpu_work)
		command := make_gameplay_command(connection.id, request.client_seq, .Aim)
		command.aim_angle = request.angle
		if !enqueue_game_command(command) {
			return {data = encode_json_line(shared.make_error_response(request.seq, "server command queue is full"))}
		}
		connection.receives_snapshots = true
		return {release_only = true}

	case .Shoot:
		request: shared.Shoot_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil || connection == nil {
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid shoot request"))}
		}
		simulate_cpu_work(cpu_work)
		command := make_gameplay_command(connection.id, request.client_seq, .Shoot)
		command.target_player_id = request.target_player_id
		if !enqueue_game_command(command) {
			return {data = encode_json_line(shared.make_error_response(request.seq, "server command queue is full"))}
		}
		connection.receives_snapshots = true
		return {release_only = true}

	case .Use_Item:
		request: shared.Use_Item_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil || connection == nil {
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid use item request"))}
		}
		simulate_cpu_work(cpu_work)
		command := make_gameplay_command(connection.id, request.client_seq, .Use_Item)
		command.item_id = request.item_id
		if !enqueue_game_command(command) {
			return {data = encode_json_line(shared.make_error_response(request.seq, "server command queue is full"))}
		}
		connection.receives_snapshots = true
		return {release_only = true}

	case .Buy:
		request: shared.Buy_Request
		when SPALL_PROFILE {
			profile_begin("json.decode_request")
		}
		err := shared.decode_json(line, &request)
		when SPALL_PROFILE {
			profile_end()
		}
		if err != nil || connection == nil {
			return {data = encode_json_line(shared.make_error_response(envelope.seq, "invalid buy request"))}
		}
		simulate_cpu_work(cpu_work)
		command := make_gameplay_command(connection.id, request.client_seq, .Buy)
		command.product_id = request.product_id
		if !enqueue_game_command(command) {
			return {data = encode_json_line(shared.make_error_response(request.seq, "server command queue is full"))}
		}
		connection.receives_snapshots = true
		return {release_only = true}

	case:
		return {data = encode_json_line(shared.make_error_response(envelope.seq, "unknown message kind"))}
	}
}

make_gameplay_command :: proc(connection_id: u64, client_seq: u32, kind: shared.Command_Kind) -> Game_Command {
	return Game_Command {
		connection_id = connection_id,
		client_seq = client_seq,
		recv_tick = sync.atomic_load(&current_server_tick),
		kind = kind,
	}
}

enqueue_system_command :: proc(connection_id: u64, kind: shared.Command_Kind) {
	command := Game_Command {
		connection_id = connection_id,
		recv_tick = sync.atomic_load(&current_server_tick),
		kind = kind,
	}
	if !enqueue_game_command(command) {
		log.warn("game command queue full; dropping system command", "connection_id", connection_id, "kind", kind)
	}
}

enqueue_game_command :: proc(command: Game_Command) -> bool {
	command_ptr := alloc_game_command()
	command_ptr^ = command
	command_ptr.is_pooled = true
	if !chan.try_send(game_command_sender, command_ptr) {
		free_game_command(command_ptr)
		return false
	}
	return true
}

destroy_simulation_state :: proc(sim: ^Simulation_State) {
	delete(sim.world.players)
	delete(sim.world.npcs)
	delete(sim.future_commands)
	delete(sim.ready_commands)
	delete(sim.gameplay_commands)
	delete(sim.keep_commands)
}

run_simulation_tick :: proc(sim: ^Simulation_State, command_receiver: chan.Chan(^Game_Command, .Recv), io_threads: []IO_Thread_State, server_tick: u64) {
	resize(&sim.ready_commands, 0)
	resize(&sim.gameplay_commands, 0)

	pull_ready_future_commands(sim, server_tick)
	drain_game_command_queue(sim, command_receiver, server_tick)
	process_system_commands(sim)
	process_gameplay_commands(sim)
	simulate_world(sim, server_tick)

	if should_broadcast_world(&sim.world, server_tick) {
		broadcast_world_snapshot(&sim.world, io_threads, server_tick)
	}
}

simulate_world :: proc(sim: ^Simulation_State, server_tick: u64) {
	tick_f := f32(server_tick)
	for i in 0 ..< len(sim.world.npcs) {
		i_f := f32(i)
		sim.world.npcs[i].x += math.sin(tick_f * 0.01 + i_f) * 0.1
		sim.world.npcs[i].y += math.cos(tick_f * 0.01 + i_f) * 0.1
	}
}

drain_game_command_queue :: proc(sim: ^Simulation_State, receiver: chan.Chan(^Game_Command, .Recv), server_tick: u64) {
	for _ in 0 ..< MAX_TOTAL_COMMANDS_PER_TICK {
		command_ptr, ok := chan.try_recv(receiver)
		if !ok {
			return
		}

		command := command_ptr^
		free_game_command(command_ptr)
		stage_command_for_tick(sim, command, server_tick)
	}
}

pull_ready_future_commands :: proc(sim: ^Simulation_State, server_tick: u64) {
	i := 0
	for i < len(sim.future_commands) {
		command := sim.future_commands[i]
		if command.target_tick > server_tick {
			i += 1
			continue
		}

		remove_future_command_at(sim, i)
		if !is_stale_gameplay_command(command, server_tick) {
			append_ready_command(sim, command)
		}
	}
}

stage_command_for_tick :: proc(sim: ^Simulation_State, command: Game_Command, server_tick: u64) {
	staged := command
	staged.target_tick = command_target_tick(staged)
	if is_system_command(staged.kind) {
		append_ready_command(sim, staged)
		return
	}

	if staged.target_tick > server_tick {
		store_future_command(sim, staged)
		return
	}
	if is_stale_gameplay_command(staged, server_tick) {
		return
	}
	append_ready_command(sim, staged)
}

command_target_tick :: proc(command: Game_Command) -> u64 {
	if is_system_command(command.kind) {
		return command.recv_tick
	}
	return command.recv_tick + INPUT_DELAY_TICKS
}

append_ready_command :: proc(sim: ^Simulation_State, command: Game_Command) -> bool {
	if !is_system_command(command.kind) && len(sim.ready_commands) >= MAX_TOTAL_COMMANDS_PER_TICK {
		return false
	}
	append(&sim.ready_commands, command)
	return true
}

store_future_command :: proc(sim: ^Simulation_State, command: Game_Command) -> bool {
	if len(sim.future_commands) >= MAX_FUTURE_COMMANDS {
		return false
	}
	count := 0
	for future in sim.future_commands {
		if future.connection_id == command.connection_id {
			count += 1
		}
	}
	if count >= MAX_FUTURE_COMMANDS_PER_CONNECTION {
		return false
	}
	append(&sim.future_commands, command)
	return true
}

remove_future_command_at :: proc(sim: ^Simulation_State, index: int) {
	last := len(sim.future_commands) - 1
	for i in index ..< last {
		sim.future_commands[i] = sim.future_commands[i + 1]
	}
	resize(&sim.future_commands, last)
}

is_stale_gameplay_command :: proc(command: Game_Command, server_tick: u64) -> bool {
	if command.target_tick >= server_tick {
		return false
	}
	return server_tick - command.target_tick > MAX_STALE_TICKS
}

process_system_commands :: proc(sim: ^Simulation_State) {
	for command in sim.ready_commands {
		#partial switch command.kind {
		case .System_Connected:
			add_player_for_connection(&sim.world, command.connection_id)
		case .System_Disconnected:
			remove_player_for_connection(&sim.world, command.connection_id)
		case:
		}
	}
}

process_gameplay_commands :: proc(sim: ^Simulation_State) {
	for command in sim.ready_commands {
		if !is_system_command(command.kind) {
			append(&sim.gameplay_commands, command)
		}
	}

	sort_gameplay_commands(sim.gameplay_commands[:])
	mark_coalesced_commands(sim)
	for command, i in sim.gameplay_commands {
		if sim.keep_commands[i] {
			apply_gameplay_command(&sim.world, command)
		}
	}
}

sort_gameplay_commands :: proc(commands: []Game_Command) {
	for i in 1 ..< len(commands) {
		key := commands[i]
		j := i
		for j > 0 && game_command_less(key, commands[j - 1]) {
			commands[j] = commands[j - 1]
			j -= 1
		}
		commands[j] = key
	}
}

game_command_less :: proc(a, b: Game_Command) -> bool {
	if a.target_tick != b.target_tick {
		return a.target_tick < b.target_tick
	}
	if a.connection_id != b.connection_id {
		return a.connection_id < b.connection_id
	}
	return a.client_seq < b.client_seq
}

mark_coalesced_commands :: proc(sim: ^Simulation_State) {
	resize(&sim.keep_commands, len(sim.gameplay_commands))
	for i in 0 ..< len(sim.keep_commands) {
		sim.keep_commands[i] = true
	}

	for command, i in sim.gameplay_commands {
		if !is_continuous_command(command.kind) {
			continue
		}
		for other, j in sim.gameplay_commands {
			if j <= i || !is_continuous_command(other.kind) {
				continue
			}
			if other.connection_id == command.connection_id && other.kind == command.kind && other.client_seq > command.client_seq {
				sim.keep_commands[i] = false
				break
			}
		}
	}
}

apply_gameplay_command :: proc(world: ^World, command: Game_Command) {
	player := find_player_by_connection_id(world, command.connection_id)
	if player == nil {
		return
	}
	if command.client_seq <= player.last_client_seq {
		return
	}

	player.last_client_seq = command.client_seq
	player.subscribed = true
	#partial switch command.kind {
	case .Move_To:
		player.x = command.x
		player.y = command.y
	case .Aim:
		player.aim_angle = command.aim_angle
	case .Shoot:
		// TODO: validate cooldowns/range/ammo before applying combat.
	case .Use_Item:
		// TODO: validate ownership/cooldowns before applying item effects.
	case .Buy:
		// TODO: validate economy rules before applying purchases.
	case:
	}
}

is_system_command :: proc(kind: shared.Command_Kind) -> bool {
	return kind == .System_Connected || kind == .System_Disconnected
}

is_continuous_command :: proc(kind: shared.Command_Kind) -> bool {
	return kind == .Move_To || kind == .Aim
}

add_player_for_connection :: proc(world: ^World, connection_id: u64) {
	if find_player_by_connection_id(world, connection_id) != nil {
		return
	}

	player := Player {
		id = world.next_player_id,
		connection_id = connection_id,
		x = 120 + f32(len(world.players)) * 32,
		y = 120,
	}
	world.next_player_id += 1
	append(&world.players, player)
}

remove_player_for_connection :: proc(world: ^World, connection_id: u64) {
	for player, i in world.players {
		if player.connection_id != connection_id {
			continue
		}
		last := len(world.players) - 1
		for j in i ..< last {
			world.players[j] = world.players[j + 1]
		}
		resize(&world.players, last)
		return
	}
}

find_player_by_connection_id :: proc(world: ^World, connection_id: u64) -> ^Player {
	for &player in world.players {
		if player.connection_id == connection_id {
			return &player
		}
	}
	return nil
}

should_broadcast_world :: proc(world: ^World, server_tick: u64) -> bool {
	if server_tick % BROADCAST_INTERVAL_TICKS != 0 {
		return false
	}
	for player in world.players {
		if player.subscribed {
			return true
		}
	}
	return false
}

broadcast_world_snapshot :: proc(world: ^World, io_threads: []IO_Thread_State, server_tick: u64) {
	master := encode_world_snapshot(world, server_tick)
	if master == nil {
		return
	}

	// Count reachable shards first so we can set remaining_sends accurately.
	reachable := 0
	for i in 0 ..< len(io_threads) {
		if io_threads[i].loop != nil {
			reachable += 1
		}
	}
	if reachable == 0 {
		delete(master)
		return
	}

	message := new(Broadcast_Shard_Message)
	message.data = master
	sync.atomic_store(&message.remaining_sends, reachable)

	for i in 0 ..< len(io_threads) {
		io_thread := &io_threads[i]
		if io_thread.loop == nil {
			continue
		}
		if !chan.try_send(io_thread.broadcast_sender, message) {
			// Shard queue full — decrement our pre-counted ref.
			if sync.atomic_sub(&message.remaining_sends, 1) == 1 {
				delete(message.data)
				free(message)
			}
			continue
		}
		schedule_io_thread_task_drain(io_thread)
	}
}

encode_world_snapshot :: proc(world: ^World, server_tick: u64) -> []byte {
	players := make([]shared.Player_Snapshot, len(world.players))
	defer delete(players)
	for player, i in world.players {
		players[i] = shared.Player_Snapshot {
			player_id = player.id,
			connection_id = player.connection_id,
			x = player.x,
			y = player.y,
			aim_angle = player.aim_angle,
		}
	}

	snapshot := shared.make_world_snapshot_response(0, server_tick, players)
	return encode_json_line(snapshot)
}

drain_broadcasts :: proc(io_thread: ^IO_Thread_State) {
	for {
		message, ok := chan.try_recv(io_thread.broadcast_receiver)
		if !ok {
			return
		}

		io_thread.metrics.broadcast_tasks += 1
		schedule_broadcast_message(io_thread, message)
	}
}

schedule_broadcast_message :: proc(io_thread: ^IO_Thread_State, message: ^Broadcast_Shard_Message) {
	if message == nil || message.data == nil {
		if message != nil {
			free(message)
		}
		return
	}

	// Count eligible targets first (no allocation needed).
	target_count := 0
	for connection in io_thread.connections {
		if connection != nil && !connection.closed && !connection.busy && connection.receives_snapshots {
			target_count += 1
		}
	}

	if target_count == 0 {
		delete(message.data)
		free(message)
		return
	}

	sync.atomic_store(&message.remaining_sends, target_count)
	io_thread.metrics.broadcast_sends += target_count
	for connection in io_thread.connections {
		if connection != nil && !connection.closed && !connection.busy && connection.receives_snapshots {
			nbio.send_poly(connection.socket, {message.data}, message, on_broadcast_sent, l=connection.loop)
		}
	}
}

on_broadcast_sent :: proc(op: ^nbio.Operation, message: ^Broadcast_Shard_Message) {
	if message == nil {
		return
	}
	if op.send.err != nil {
		log.warn("broadcast send failed:", op.send.err)
	}
	if sync.atomic_sub(&message.remaining_sends, 1) == 1 {
		delete(message.data)
		free(message)
	}
}

remove_io_connection :: proc(io_thread: ^IO_Thread_State, connection: ^Connection) {
	if io_thread == nil || connection == nil {
		return
	}
	for candidate, i in io_thread.connections {
		if candidate != connection {
			continue
		}
		last := len(io_thread.connections) - 1
		for j in i ..< last {
			io_thread.connections[j] = io_thread.connections[j + 1]
		}
		resize(&io_thread.connections, last)
		return
	}
}

simulate_cpu_work :: proc(duration: time.Duration) {
	if duration <= 0 {
		return
	}

	when SPALL_PROFILE {
		profile_begin("request.cpu_work")
	}
	defer {
		when SPALL_PROFILE {
			profile_end()
		}
	}

	started := time.tick_now()
	state := sync.atomic_load_explicit(&cpu_work_sink, .Relaxed)
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}

	for {
		for _ in 0 ..< CPU_WORK_BATCH_SIZE {
			state = state ~ (state << 13)
			state = state ~ (state >> 7)
			state = state ~ (state << 17)
		}
		if time.tick_since(started) >= duration {
			break
		}
	}

	sync.atomic_store_explicit(&cpu_work_sink, state, .Relaxed)
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

	b: strings.Builder
	strings.builder_init(&b)
	opt := shared.JSON_OPTIONS
	if err := json.marshal_to_builder(&b, message, &opt); err != nil {
		strings.builder_destroy(&b)
		return nil
	}
	strings.write_byte(&b, '\n')
	return b.buf[:]
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

	if response == nil {
		return
	}

	connection := response.connection
	if connection == nil {
		delete(response.data)
		free_response_item(response)
		return
	}
	if connection.closed {
		delete(response.data)
		free_response_item(response)
		return
	}
	if response.release_only {
		free_response_item(response)
		complete_connection_request(connection)
		return
	}
	if response.data == nil {
		free_response_item(response)
		close_connection(connection)
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
	free_response_item(response)

	if connection.closed {
		return
	}
	connection.io_thread.metrics.send_events += 1

	if op.send.err != nil {
		close_connection(connection)
		return
	}

	connection.io_thread.metrics.responses_sent += 1
	complete_connection_request(connection)
}

complete_connection_request :: proc(connection: ^Connection) {
	if connection == nil || connection.closed {
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
	enqueue_system_command(connection.id, .System_Disconnected)
	for line in connection.pending_lines {
		free_line_buffer(line)
	}
	delete(connection.pending_lines)
	remove_io_connection(connection.io_thread, connection)
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
