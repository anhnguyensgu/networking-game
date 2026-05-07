package main

import shared "../shared"

import "core:flags"
import "core:fmt"
import "core:net"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"

Bench_Options :: struct {
	clients:      int `usage:"Concurrent request clients."`,
	requests:     int `usage:"Maximum requests per client. Zero means run until duration-ms."`,
	timeout_ms:   int `args:"name=timeout-ms" usage:"Per-socket send/receive timeout in milliseconds."`,
	idle_clients: int `args:"name=idle-clients" usage:"Connected clients that stay idle before the benchmark starts."`,
	idle_ms:      int `args:"name=idle-ms" usage:"How long idle clients keep their sockets open."`,
	duration_ms:  int `args:"name=duration-ms" usage:"How long request clients should keep sending requests."`,
}

Counters :: struct {
	ok:          int,
	failed:      int,
	dial_failed: int,
}

Request_Worker :: struct {
	index:        int,
	max_requests: int,
	timeout_ms:   int,
	duration:     time.Duration,
	start:        ^sync.Barrier,
	counters:     ^Counters,
}

Idle_Worker :: struct {
	sleep_ms: int,
}

main :: proc() {
	options := Bench_Options {
		clients      = 32,
		requests     = 0,
		timeout_ms   = 2000,
		idle_clients = 0,
		idle_ms      = 5000,
		duration_ms  = 10000,
	}
	flags.parse_or_exit(&options, os.args, .Unix)

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

	counters: Counters
	start: sync.Barrier
	sync.barrier_init(&start, options.clients + 1)

	workers := make([]Request_Worker, options.clients)
	defer delete(workers)

	threads := make([]^thread.Thread, options.clients)
	defer delete(threads)

	for i in 0 ..< options.clients {
		workers[i] = Request_Worker {
			index        = i,
			max_requests = options.requests,
			timeout_ms   = options.timeout_ms,
			duration     = time.Duration(options.duration_ms) * time.Millisecond,
			start        = &start,
			counters     = &counters,
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
	ok := sync.atomic_load(&counters.ok)
	failed := sync.atomic_load(&counters.failed)
	dial_failed := sync.atomic_load(&counters.dial_failed)
	attempts := ok + failed
	seconds := time.duration_seconds(elapsed)
	requests_per_second := f64(ok) / seconds if seconds > 0 else 0

	fmt.printfln(
		"server=%s clients=%d duration_ms=%d max_requests/client=%d attempts=%d ok=%d failed=%d dial_failed=%d elapsed_ms=%.2f req/s=%.2f",
		shared.SERVER_ADDRESS,
		options.clients,
		options.duration_ms,
		options.requests,
		attempts,
		ok,
		failed,
		dial_failed,
		time.duration_milliseconds(elapsed),
		requests_per_second,
	)

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

	ok, failed, dial_failed := run_request_worker(worker)
	sync.atomic_add(&worker.counters.ok, ok)
	sync.atomic_add(&worker.counters.failed, failed)
	sync.atomic_add(&worker.counters.dial_failed, dial_failed)
}

run_request_worker :: proc(worker: ^Request_Worker) -> (ok, failed, dial_failed: int) {
	started := time.tick_now()
	request_index := 0
	for time.tick_since(started) < worker.duration {
		if worker.max_requests > 0 && request_index >= worker.max_requests {
			break
		}

		seq := u64(worker.index + 1) * 1_000_000_000 + u64(request_index + 1)
		request_ok, request_dial_failed := run_single_request(seq, worker.timeout_ms)
		if request_ok {
			ok += 1
		} else {
			failed += 1
			if request_dial_failed {
				dial_failed += 1
			}
			time.sleep(1 * time.Millisecond)
		}

		request_index += 1
	}

	return
}

run_single_request :: proc(seq: u64, timeout_ms: int) -> (ok, dial_failed: bool) {
	socket, dial_err := net.dial_tcp(shared.SERVER_ADDRESS)
	if dial_err != nil {
		return false, true
	}
	defer net.close(socket)

	timeout := time.Duration(timeout_ms) * time.Millisecond
	_ = net.set_option(socket, .Receive_Timeout, timeout)
	_ = net.set_option(socket, .Send_Timeout, timeout)

	request := shared.make_get_world_map_request(seq)
	if !shared.send_json_line(socket, request) {
		return false, false
	}

	line_buf: [shared.MAX_LINE_BYTES]byte
	line, read_ok := shared.read_json_line(socket, line_buf[:])
	if !read_ok {
		return false, false
	}

	envelope, envelope_err := shared.decode_envelope(line)
	if envelope_err != nil || envelope.kind != .World_Map || envelope.seq != seq {
		return false, false
	}

	response: shared.World_Map_Response
	if err := shared.decode_json(line, &response); err != nil {
		return false, false
	}
	shared.destroy_world_map_response(&response)

	return true, false
}
