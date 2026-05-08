package main

import shared "../shared"

import "core:flags"
import "core:fmt"
import "core:net"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"

MODE_ROUNDTRIP :: "roundtrip"
MODE_PIPELINE :: "pipeline"
MODE_BURST :: "burst"
READ_CHUNK_SIZE :: 4096

Bench_Options :: struct {
	mode:           string `usage:"Benchmark mode: roundtrip, pipeline, or burst."`,
	clients:        int    `usage:"Concurrent request clients."`,
	requests:       int    `usage:"Maximum requests per client. Zero means run until duration-ms."`,
	timeout_ms:     int    `args:"name=timeout-ms" usage:"Per-socket send/receive timeout in milliseconds."`,
	idle_clients:   int    `args:"name=idle-clients" usage:"Connected clients that stay idle before the benchmark starts."`,
	idle_ms:        int    `args:"name=idle-ms" usage:"How long idle clients keep their sockets open."`,
	duration_ms:    int    `args:"name=duration-ms" usage:"How long request clients should keep sending requests."`,
	pipeline_depth: int    `args:"name=pipeline-depth" usage:"Requests sent before reading responses in pipeline mode."`,
	burst_size:     int    `args:"name=burst-size" usage:"Requests sent per client burst in burst mode."`,
	burst_pause_ms: int    `args:"name=burst-pause-ms" usage:"Pause between bursts after responses are read."`,
}

Worker_Result :: struct {
	ok:          int,
	failed:      int,
	dial_failed: int,
	latency_total: time.Duration,
	latency_max:   time.Duration,
}

Request_Worker :: struct {
	index:          int,
	mode:           string,
	max_requests:   int,
	timeout_ms:     int,
	duration:       time.Duration,
	pipeline_depth: int,
	burst_size:     int,
	burst_pause:    time.Duration,
	start:          ^sync.Barrier,
	result:         ^Worker_Result,
}

Idle_Worker :: struct {
	sleep_ms: int,
}

Buffered_Line_Reader :: struct {
	pending: [dynamic]byte,
}

main :: proc() {
	options := Bench_Options {
		mode           = MODE_ROUNDTRIP,
		clients        = 32,
		requests       = 0,
		timeout_ms     = 2000,
		idle_clients   = 0,
		idle_ms        = 5000,
		duration_ms    = 10000,
		pipeline_depth = 8,
		burst_size     = 64,
		burst_pause_ms = 100,
	}
	flags.parse_or_exit(&options, os.args, .Unix)

	if !is_valid_mode(options.mode) {
		fmt.eprintln("mode must be roundtrip, pipeline, or burst")
		os.exit(1)
	}
	if options.clients <= 0 || options.timeout_ms <= 0 || options.duration_ms <= 0 {
		fmt.eprintln("clients, timeout-ms, and duration-ms must be greater than zero")
		os.exit(1)
	}
	if options.requests < 0 {
		fmt.eprintln("requests cannot be negative")
		os.exit(1)
	}
	if options.idle_clients < 0 || options.idle_ms < 0 {
		fmt.eprintln("idle-clients and idle-ms cannot be negative")
		os.exit(1)
	}
	if options.pipeline_depth <= 0 || options.burst_size <= 0 || options.burst_pause_ms < 0 {
		fmt.eprintln("pipeline-depth and burst-size must be greater than zero; burst-pause-ms cannot be negative")
		os.exit(1)
	}

	idle_threads := start_idle_clients(options.idle_clients, options.idle_ms)
	defer {
		for t in idle_threads {
			thread.destroy(t)
		}
		delete(idle_threads)
	}

	if options.idle_clients > 0 {
		time.sleep(100 * time.Millisecond)
	}

	start: sync.Barrier
	sync.barrier_init(&start, options.clients + 1)

	workers := make([]Request_Worker, options.clients)
	defer delete(workers)

	results := make([]Worker_Result, options.clients)
	defer delete(results)

	threads := make([]^thread.Thread, options.clients)
	defer delete(threads)

	for i in 0 ..< options.clients {
		workers[i] = Request_Worker {
			index          = i,
			mode           = options.mode,
			max_requests   = options.requests,
			timeout_ms     = options.timeout_ms,
			duration       = time.Duration(options.duration_ms) * time.Millisecond,
			pipeline_depth = options.pipeline_depth,
			burst_size     = options.burst_size,
			burst_pause    = time.Duration(options.burst_pause_ms) * time.Millisecond,
			start          = &start,
			result         = &results[i],
		}

		t := thread.create(request_worker_proc)
		if t == nil {
			fmt.eprintfln("failed to create request worker %d", i)
			os.exit(1)
		}
		t.data = rawptr(&workers[i])
		threads[i] = t
	}

	for t in threads {
		thread.start(t)
	}

	start_tick := time.tick_now()
	_ = sync.barrier_wait(&start)

	for t in threads {
		thread.destroy(t)
	}

	elapsed := time.tick_since(start_tick)
	ok, failed, dial_failed, latency_total, latency_max := summarize_results(results)
	attempts := ok + failed
	seconds := time.duration_seconds(elapsed)
	requests_per_second := f64(ok) / seconds if seconds > 0 else 0
	avg_latency_ms := 0.0
	if ok > 0 {
		avg_latency_ms = time.duration_milliseconds(latency_total) / f64(ok)
	}

	fmt.printfln(
		"server=%s mode=%s clients=%d idle_clients=%d duration_ms=%d max_requests/client=%d pipeline_depth=%d burst_size=%d burst_pause_ms=%d attempts=%d ok=%d failed=%d dial_failed=%d elapsed_ms=%.2f req/s=%.2f avg_latency_ms=%.3f max_latency_ms=%.3f",
		shared.SERVER_ADDRESS,
		options.mode,
		options.clients,
		options.idle_clients,
		options.duration_ms,
		options.requests,
		options.pipeline_depth,
		options.burst_size,
		options.burst_pause_ms,
		attempts,
		ok,
		failed,
		dial_failed,
		time.duration_milliseconds(elapsed),
		requests_per_second,
		avg_latency_ms,
		time.duration_milliseconds(latency_max),
	)

}

is_valid_mode :: proc(mode: string) -> bool {
	return mode == MODE_ROUNDTRIP || mode == MODE_PIPELINE || mode == MODE_BURST
}

summarize_results :: proc(results: []Worker_Result) -> (
	ok, failed, dial_failed: int,
	latency_total, latency_max: time.Duration,
) {
	for result in results {
		ok += result.ok
		failed += result.failed
		dial_failed += result.dial_failed
		latency_total += result.latency_total
		if result.latency_max > latency_max {
			latency_max = result.latency_max
		}
	}
	return
}

start_idle_clients :: proc(count, sleep_ms: int) -> []^thread.Thread {
	threads := make([]^thread.Thread, count)
	for i in 0 ..< count {
		worker := new(Idle_Worker)
		worker.sleep_ms = sleep_ms

		t := thread.create(idle_worker_proc)
		if t == nil {
			fmt.eprintfln("failed to create idle worker %d", i)
			os.exit(1)
		}

		t.data = rawptr(worker)
		threads[i] = t
		thread.start(t)
	}
	return threads
}

idle_worker_proc :: proc(t: ^thread.Thread) {
	worker := (^Idle_Worker)(t.data)
	defer free(worker)

	socket, dial_err := net.dial_tcp(shared.SERVER_ADDRESS)
	if dial_err != nil {
		return
	}
	defer net.close(socket)

	time.sleep(time.Duration(worker.sleep_ms) * time.Millisecond)
}

request_worker_proc :: proc(t: ^thread.Thread) {
	worker := (^Request_Worker)(t.data)
	_ = sync.barrier_wait(worker.start)

	worker.result^ = run_request_worker(worker)
}

run_request_worker :: proc(worker: ^Request_Worker) -> Worker_Result {
	result: Worker_Result
	socket, dial_err := net.dial_tcp(shared.SERVER_ADDRESS)
	if dial_err != nil {
		result.failed = 1
		result.dial_failed = 1
		return result
	}
	defer net.close(socket)

	timeout := time.Duration(worker.timeout_ms) * time.Millisecond
	_ = net.set_option(socket, .Receive_Timeout, timeout)
	_ = net.set_option(socket, .Send_Timeout, timeout)

	reader := Buffered_Line_Reader {
		pending = make([dynamic]byte),
	}
	defer delete(reader.pending)

	started := time.tick_now()
	switch worker.mode {
	case MODE_ROUNDTRIP:
		run_batched_worker(worker, socket, &reader, started, 1, 0, &result)
	case MODE_PIPELINE:
		run_batched_worker(worker, socket, &reader, started, worker.pipeline_depth, 0, &result)
	case MODE_BURST:
		run_batched_worker(worker, socket, &reader, started, worker.burst_size, worker.burst_pause, &result)
	case:
		result.failed = 1
	}

	return result
}

run_batched_worker :: proc(
	worker: ^Request_Worker,
	socket: net.TCP_Socket,
	reader: ^Buffered_Line_Reader,
	started: time.Tick,
	batch_size: int,
	batch_pause: time.Duration,
	result: ^Worker_Result,
) {
	seqs := make([]u64, batch_size)
	defer delete(seqs)
	send_ticks := make([]time.Tick, batch_size)
	defer delete(send_ticks)

	request_index := 0
	for time.tick_since(started) < worker.duration {
		if worker.max_requests > 0 && request_index >= worker.max_requests {
			break
		}

		sent := 0
		for sent < batch_size && time.tick_since(started) < worker.duration {
			if worker.max_requests > 0 && request_index >= worker.max_requests {
				break
			}

			seq := u64(worker.index + 1) * 1_000_000_000 + u64(request_index + 1)
			send_ticks[sent] = time.tick_now()
			if !send_request(socket, seq) {
				result.failed += 1
				return
			}
			seqs[sent] = seq
			sent += 1
			request_index += 1
		}

		if sent == 0 {
			break
		}

		for i in 0 ..< sent {
			if !read_world_map_response(socket, reader, seqs[i]) {
				result.failed += 1
				return
			}

			result.ok += 1
			record_latency(result, time.tick_since(send_ticks[i]))
		}

		if batch_pause > 0 && time.tick_since(started) < worker.duration {
			time.sleep(batch_pause)
		}
	}
}

send_request :: proc(socket: net.TCP_Socket, seq: u64) -> bool {
	request := shared.make_get_world_map_request(seq)
	return shared.send_json_line(socket, request)
}

read_world_map_response :: proc(socket: net.TCP_Socket, reader: ^Buffered_Line_Reader, seq: u64) -> bool {
	line_buf: [shared.MAX_LINE_BYTES]byte
	line, read_ok := read_json_line_buffered(socket, reader, line_buf[:])
	if !read_ok {
		return false
	}

	envelope, envelope_err := shared.decode_envelope(line)
	if envelope_err != nil || envelope.kind != .World_Map || envelope.seq != seq {
		return false
	}

	response: shared.World_Map_Response
	if err := shared.decode_json(line, &response); err != nil {
		return false
	}
	shared.destroy_world_map_response(&response)

	return true
}

record_latency :: proc(result: ^Worker_Result, latency: time.Duration) {
	result.latency_total += latency
	if latency > result.latency_max {
		result.latency_max = latency
	}
}

read_json_line_buffered :: proc(
	socket: net.TCP_Socket,
	reader: ^Buffered_Line_Reader,
	out: []byte,
) -> (line: []byte, ok: bool) {
	for {
		if line_end, found := find_line_end(reader.pending[:]); found {
			if line_end > len(out) {
				return nil, false
			}
			copy(out[:line_end], reader.pending[:line_end])
			consume_pending(reader, line_end + 1)
			return out[:line_end], true
		}

		buf: [READ_CHUNK_SIZE]byte
		n, err := net.recv_tcp(socket, buf[:])
		if err != nil || n == 0 {
			return nil, false
		}
		for b in buf[:n] {
			append(&reader.pending, b)
		}
		if _, found := find_line_end(reader.pending[:]); !found && len(reader.pending) > len(out) {
			return nil, false
		}
	}
}

find_line_end :: proc(data: []byte) -> (index: int, found: bool) {
	for b, i in data {
		if b == '\n' {
			return i, true
		}
	}
	return 0, false
}

consume_pending :: proc(reader: ^Buffered_Line_Reader, count: int) {
	remaining := len(reader.pending) - count
	for i in 0 ..< remaining {
		reader.pending[i] = reader.pending[i + count]
	}
	resize(&reader.pending, remaining)
}
