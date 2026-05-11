const std = @import("std");
const c = @cImport({
    @cInclude("sys/socket.h");
    @cInclude("netinet/in.h");
    @cInclude("arpa/inet.h");
    @cInclude("unistd.h");
    @cInclude("pthread.h");
    @cInclude("sys/time.h");
    @cInclude("time.h");
});
const json = std.json;
const mem = std.mem;
const math = std.math;
const Thread = std.Thread;

const WORLD_WIDTH: f64 = 960.0;
const WORLD_HEIGHT: f64 = 640.0;
const PLAYER_SPEED: f64 = 220.0;
const MAX_LINE_BYTES: usize = 128 * 1024;

const MessageKind = enum {
    Unknown,
    Get_World_Map,
    World_Map,
    Select_Base,
    Error,
    Move_To,
    Aim,
    Shoot,
    Use_Item,
    Buy,
    World_Snapshot,

    pub fn fromString(s: []const u8) MessageKind {
        if (mem.eql(u8, s, "Get_World_Map")) return .Get_World_Map;
        if (mem.eql(u8, s, "Move_To")) return .Move_To;
        if (mem.eql(u8, s, "Aim")) return .Aim;
        return .Unknown;
    }
};

const EnemyBaseView = struct {
    id: i32,
    x: f32,
    y: f32,
    level: i32,
    name: []const u8,
};

const PlayerView = struct {
    id: u64,
    connection_id: u64,
    x: f64,
    y: f64,
    aim_x: f64,
    aim_y: f64,
    alive: bool,
};

const WorldSnapshot = struct {
    kind: []const u8 = "World_Snapshot",
    tick: u64,
    players: []const PlayerView,
};

const WorldMapResponse = struct {
    kind: []const u8 = "World_Map",
    seq: u64,
    bases: []const EnemyBaseView,
};

const Envelope = struct {
    kind: []const u8,
    seq: ?u64 = null,
    client_seq: ?u32 = null,
};

const MoveToRequest = struct {
    kind: []const u8,
    seq: u64,
    client_seq: u32,
    x: f64,
    y: f64,
};

const AimRequest = struct {
    kind: []const u8,
    seq: u64,
    client_seq: u32,
    x: f64,
    y: f64,
};

const Player = struct {
    id: u64,
    connection_id: u64,
    x: f64,
    y: f64,
    target_x: f64,
    target_y: f64,
    aim_x: f64,
    aim_y: f64,
    alive: bool,
    last_processed_seq: u32,
};

const CommandKind = enum {
    System_Connected,
    System_Disconnected,
    Move_To,
    Aim,
};

const GameCommand = struct {
    connection_id: u64,
    client_seq: u32,
    kind: CommandKind,
    x: f64 = 0,
    y: f64 = 0,
};

const Mutex = struct {
    inner: c.pthread_mutex_t,
    pub fn init() Mutex {
        var m: Mutex = undefined;
        _ = c.pthread_mutex_init(&m.inner, null);
        return m;
    }
    pub fn lock(self: *Mutex) void {
        _ = c.pthread_mutex_lock(&self.inner);
    }
    pub fn unlock(self: *Mutex) void {
        _ = c.pthread_mutex_unlock(&self.inner);
    }
};

const Server = struct {
    allocator: mem.Allocator,
    next_connection_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    current_tick: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    
    connections: std.AutoHashMap(u64, i32),
    connections_mutex: Mutex,
    
    command_queue: std.ArrayListUnmanaged(GameCommand),
    command_mutex: Mutex,

    pub fn init(allocator: mem.Allocator) Server {
        return .{
            .allocator = allocator,
            .connections = std.AutoHashMap(u64, i32).init(allocator),
            .connections_mutex = Mutex.init(),
            .command_queue = .empty,
            .command_mutex = Mutex.init(),
        };
    }

    pub fn enqueueCommand(self: *Server, cmd: GameCommand) void {
        self.command_mutex.lock();
        defer self.command_mutex.unlock();
        self.command_queue.append(self.allocator, cmd) catch return;
    }
};

const NPC_COUNT = 100000;

const NPC = struct {
    x: f64,
    y: f64,
};

const World = struct {
    next_player_id: u64 = 1,
    players: std.AutoHashMap(u64, Player),
    conn_to_player: std.AutoHashMap(u64, u64),
    npcs: []NPC,
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) World {
        const npcs = allocator.alloc(NPC, NPC_COUNT) catch unreachable;
        for (npcs, 0..) |*npc, i| {
            npc.* = .{ .x = @as(f64, @floatFromInt(i % 1000)), .y = @as(f64, @floatFromInt(i / 1000)) };
        }
        return .{
            .players = std.AutoHashMap(u64, Player).init(allocator),
            .conn_to_player = std.AutoHashMap(u64, u64).init(allocator),
            .npcs = npcs,
            .allocator = allocator,
        };
    }

    pub fn applyCommand(self: *World, cmd: GameCommand) void {
        switch (cmd.kind) {
            .System_Connected => {
                if (self.conn_to_player.contains(cmd.connection_id)) return;
                const pid = self.next_player_id;
                self.next_player_id += 1;
                const spawn_index = @as(f64, @floatFromInt((pid - 1) % 8));
                const p = Player{
                    .id = pid,
                    .connection_id = cmd.connection_id,
                    .x = 120.0 + spawn_index * 60.0,
                    .y = 120.0 + spawn_index * 32.0,
                    .target_x = 120.0 + spawn_index * 60.0,
                    .target_y = 120.0 + spawn_index * 32.0,
                    .aim_x = 1.0,
                    .aim_y = 0.0,
                    .alive = true,
                    .last_processed_seq = 0,
                };
                self.players.put(pid, p) catch return;
                self.conn_to_player.put(cmd.connection_id, pid) catch return;
            },
            .System_Disconnected => {
                if (self.conn_to_player.get(cmd.connection_id)) |pid| {
                    _ = self.conn_to_player.remove(cmd.connection_id);
                    _ = self.players.remove(pid);
                }
            },
            .Move_To => {
                if (self.conn_to_player.get(cmd.connection_id)) |pid| {
                    if (self.players.getPtr(pid)) |p| {
                        if (cmd.client_seq > p.last_processed_seq) {
                            p.last_processed_seq = cmd.client_seq;
                            p.target_x = math.clamp(cmd.x, 0, WORLD_WIDTH);
                            p.target_y = math.clamp(cmd.y, 0, WORLD_HEIGHT);
                        }
                    }
                }
            },
            .Aim => {
                if (self.conn_to_player.get(cmd.connection_id)) |pid| {
                    if (self.players.getPtr(pid)) |p| {
                        if (cmd.client_seq > p.last_processed_seq) {
                            p.last_processed_seq = cmd.client_seq;
                            const length = @sqrt(cmd.x * cmd.x + cmd.y * cmd.y);
                            if (length > 0) {
                                p.aim_x = cmd.x / length;
                                p.aim_y = cmd.y / length;
                            }
                        }
                    }
                }
            },
        }
    }

    pub fn simulate(self: *World, dt: f64, current_tick: u64) void {
        const step = PLAYER_SPEED * dt;
        var it = self.players.valueIterator();
        while (it.next()) |p| {
            const dx = p.target_x - p.x;
            const dy = p.target_y - p.y;
            const dist = @sqrt(dx * dx + dy * dy);
            if (dist == 0) continue;
            if (dist <= step) {
                p.x = p.target_x;
                p.y = p.target_y;
            } else {
                p.x += dx / dist * step;
                p.y += dy / dist * step;
            }
        }

        // Intensive NPC simulation
        const tick_f = @as(f64, @floatFromInt(current_tick));
        for (self.npcs, 0..) |*npc, i| {
            const i_f = @as(f64, @floatFromInt(i));
            npc.x += @sin(tick_f * 0.01 + i_f) * 0.1;
            npc.y += @cos(tick_f * 0.01 + i_f) * 0.1;
        }
    }
};

const enemy_bases = [_]EnemyBaseView{
    .{ .id = 1, .x = 180, .y = 160, .level = 2, .name = "Stone Reef" },
    .{ .id = 2, .x = 420, .y = 260, .level = 4, .name = "Iron Cove" },
    .{ .id = 3, .x = 660, .y = 180, .level = 6, .name = "Storm Pier" },
    .{ .id = 4, .x = 540, .y = 420, .level = 8, .name = "Crab Harbor" },
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const fd = c.socket(c.AF_INET, c.SOCK_STREAM, 0);
    if (fd < 0) return error.SocketFailed;
    defer _ = c.close(fd);

    const on: i32 = 1;
    _ = c.setsockopt(fd, c.SOL_SOCKET, c.SO_REUSEADDR, &on, @sizeOf(i32));

    var addr: c.sockaddr_in = undefined;
    @memset(mem.asBytes(&addr), 0);
    addr.sin_family = @intCast(c.AF_INET);
    addr.sin_port = c.htons(43120);
    addr.sin_addr.s_addr = c.INADDR_ANY;
    // On Darwin, sin_len is the first field.
    if (@hasField(c.sockaddr_in, "sin_len")) {
        addr.sin_len = @sizeOf(c.sockaddr_in);
    }

    if (c.bind(fd, @ptrCast(&addr), @sizeOf(c.sockaddr_in)) < 0) return error.BindFailed;
    if (c.listen(fd, 128) < 0) return error.ListenFailed;

    var server = Server.init(allocator);

    std.debug.print("zig server listening on 127.0.0.1:43120\n", .{});

    const sim_thread = try Thread.spawn(.{}, simulationLoop, .{&server});
    sim_thread.detach();

    while (true) {
        var client_addr: c.sockaddr_in = undefined;
        var client_addr_len: c.socklen_t = @sizeOf(c.sockaddr_in);
        const client_fd = c.accept(fd, @ptrCast(&client_addr), &client_addr_len);
        if (client_fd < 0) continue;
        
        const conn_id = server.next_connection_id.fetchAdd(1, .seq_cst);
        
        server.connections_mutex.lock();
        try server.connections.put(conn_id, client_fd);
        server.connections_mutex.unlock();

        server.enqueueCommand(.{ .connection_id = conn_id, .client_seq = 0, .kind = .System_Connected });

        const t = try Thread.spawn(.{}, handleConnection, .{ &server, conn_id, client_fd });
        t.detach();
    }
}

fn simulationLoop(server: *Server) !void {
    var world = World.init(server.allocator);
    const tick_duration_ns = @as(u64, @intFromFloat(1e9 / 60.0));
    const dt = 1.0 / 60.0;

    while (true) {
        var start_ts: c.timespec = undefined;
        _ = c.clock_gettime(c.CLOCK_MONOTONIC, &start_ts);

        const tick = server.current_tick.fetchAdd(1, .seq_cst);

        server.command_mutex.lock();
        if (server.command_queue.items.len > 0) {
            const cmds = try server.command_queue.toOwnedSlice(server.allocator);
            server.command_mutex.unlock();
            defer server.allocator.free(cmds);
            for (cmds) |cmd| {
                world.applyCommand(cmd);
            }
        } else {
            server.command_mutex.unlock();
        }

        world.simulate(dt, tick);

        var end_ts: c.timespec = undefined;
        _ = c.clock_gettime(c.CLOCK_MONOTONIC, &end_ts);
        const elapsed_ns = @as(u64, @intCast((end_ts.tv_sec - start_ts.tv_sec) * 1_000_000_000 + (end_ts.tv_nsec - start_ts.tv_nsec)));
        
        if (elapsed_ns < tick_duration_ns) {
            _ = c.usleep(@as(c_uint, @intCast((tick_duration_ns - elapsed_ns) / 1000)));
        }
    }
}

fn handleConnection(server: *Server, id: u64, fd: i32) !void {
    defer {
        server.connections_mutex.lock();
        _ = server.connections.remove(id);
        server.connections_mutex.unlock();
        server.enqueueCommand(.{ .connection_id = id, .client_seq = 0, .kind = .System_Disconnected });
        _ = c.close(fd);
    }

    var buffer: [MAX_LINE_BYTES]u8 = undefined;
    var line_start: usize = 0;

    while (true) {
        const bytes_read = c.recv(fd, &buffer[line_start], MAX_LINE_BYTES - line_start, 0);
        if (bytes_read <= 0) break;
        
        const total_len = line_start + @as(usize, @intCast(bytes_read));
        var current_pos: usize = 0;
        
        while (mem.indexOfScalar(u8, buffer[current_pos..total_len], '\n')) |newline_idx| {
            const line = buffer[current_pos .. current_pos + newline_idx];
            if (line.len > 0) {
                processLine(server, id, fd, line) catch {};
            }
            current_pos += newline_idx + 1;
        }
        
        if (current_pos < total_len) {
            mem.copyForwards(u8, buffer[0 .. total_len - current_pos], buffer[current_pos..total_len]);
            line_start = total_len - current_pos;
        } else {
            line_start = 0;
        }
    }
}

fn processLine(server: *Server, id: u64, fd: i32, line: []const u8) !void {
    const parsed = json.parseFromSlice(Envelope, server.allocator, line, .{ .ignore_unknown_fields = true }) catch return;
    defer parsed.deinit();

    const kind = MessageKind.fromString(parsed.value.kind);
    switch (kind) {
        .Get_World_Map => {
            const seq = parsed.value.seq orelse 0;
            // Manual JSON stringify using bufPrint
            var list = std.ArrayListUnmanaged(u8).empty;
            defer list.deinit(server.allocator);
            
            var buf: [256]u8 = undefined;
            const header = try std.fmt.bufPrint(&buf, "{{\"kind\":\"World_Map\",\"seq\":{d},\"bases\":[", .{seq});
            try list.appendSlice(server.allocator, header);

            for (enemy_bases, 0..) |base, i| {
                if (i > 0) try list.append(server.allocator, ',');
                const item = try std.fmt.bufPrint(&buf, "{{\"id\":{d},\"x\":{d:.2},\"y\":{d:.2},\"level\":{d},\"name\":\"{s}\"}}", .{base.id, base.x, base.y, base.level, base.name});
                try list.appendSlice(server.allocator, item);
            }
            try list.appendSlice(server.allocator, "]}}\n");
            _ = c.send(fd, list.items.ptr, list.items.len, 0);
        },
        .Move_To => {
            const req = json.parseFromSlice(MoveToRequest, server.allocator, line, .{ .ignore_unknown_fields = true }) catch return;
            defer req.deinit();
            server.enqueueCommand(.{
                .connection_id = id,
                .client_seq = req.value.client_seq,
                .kind = .Move_To,
                .x = req.value.x,
                .y = req.value.y,
            });
        },
        .Aim => {
            const req = json.parseFromSlice(AimRequest, server.allocator, line, .{ .ignore_unknown_fields = true }) catch return;
            defer req.deinit();
            server.enqueueCommand(.{
                .connection_id = id,
                .client_seq = req.value.client_seq,
                .kind = .Aim,
                .x = req.value.x,
                .y = req.value.y,
            });
        },
        else => {},
    }
}
