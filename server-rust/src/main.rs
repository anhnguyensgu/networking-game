use clap::Parser;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, MissedTickBehavior};

const WORLD_WIDTH: f64 = 960.0;
const WORLD_HEIGHT: f64 = 640.0;
const PLAYER_SPEED: f64 = 220.0;
const MAX_LINE_BYTES: usize = 128 * 1024;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:43120")]
    addr: String,

    #[arg(long, default_value_t = 60)]
    tick_rate: u64,

    #[arg(long, default_value_t = 20)]
    broadcast_hz: u64,

    #[arg(long, default_value_t = 8192)]
    queue_size: usize,

    #[arg(long, default_value_t = 256)]
    write_queue_size: usize,

    #[arg(long, default_value_t = 2)]
    input_delay_ticks: u64,

    #[arg(long, default_value_t = 20)]
    max_stale_ticks: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum MessageKind {
    #[serde(rename = "Get_World_Map")]
    GetWorldMap,
    #[serde(rename = "World_Map")]
    WorldMap,
    #[serde(rename = "Select_Base")]
    SelectBase,
    #[serde(rename = "Error")]
    Error,
    #[serde(rename = "Move_To")]
    MoveTo,
    #[serde(rename = "Aim")]
    Aim,
    #[serde(rename = "Shoot")]
    Shoot,
    #[serde(rename = "Use_Item")]
    UseItem,
    #[serde(rename = "Buy")]
    Buy,
    #[serde(rename = "World_Snapshot")]
    WorldSnapshot,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Envelope {
    kind: MessageKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_seq: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct GetWorldMapRequest {
    seq: u64,
}

#[derive(Serialize, Debug)]
struct WorldMapResponse {
    kind: MessageKind,
    seq: u64,
    bases: Vec<EnemyBaseView>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EnemyBaseView {
    id: i32,
    x: f32,
    y: f32,
    level: i32,
    name: String,
}

#[derive(Deserialize, Debug)]
struct MoveToRequest {
    seq: u64,
    client_seq: u32,
    x: f64,
    y: f64,
}

#[derive(Deserialize, Debug)]
struct AimRequest {
    seq: u64,
    client_seq: u32,
    x: f64,
    y: f64,
}

#[derive(Deserialize, Debug)]
struct ActionRequest {
    seq: u64,
    client_seq: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    product_id: Option<String>,
}

#[derive(Serialize, Debug)]
struct ErrorResponse {
    kind: MessageKind,
    seq: u64,
    message: String,
}

#[derive(Serialize, Debug, Clone)]
struct PlayerView {
    id: u64,
    connection_id: u64,
    x: f64,
    y: f64,
    aim_x: f64,
    aim_y: f64,
    alive: bool,
}

#[derive(Serialize, Debug)]
struct WorldSnapshot {
    kind: MessageKind,
    tick: u64,
    players: Vec<PlayerView>,
}

#[derive(Debug, Clone)]
enum CommandKind {
    SystemConnected,
    SystemDisconnected,
    MoveTo,
    Aim,
    Shoot,
    UseItem,
    Buy,
}

#[derive(Debug, Clone)]
struct GameCommand {
    connection_id: u64,
    client_seq: u32,
    recv_tick: u64,
    target_tick: u64,
    kind: CommandKind,
    x: f64,
    y: f64,
    item_id: Option<String>,
    product_id: Option<String>,
}

struct Player {
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
}

const NPC_COUNT: usize = 100000;

struct NPC {
    x: f64,
    y: f64,
}

struct World {
    next_player_id: u64,
    players: HashMap<u64, Player>,
    connection_to_player: HashMap<u64, u64>,
    npcs: Vec<NPC>,
}

impl World {
    fn new() -> Self {
        let mut npcs = Vec::with_capacity(NPC_COUNT);
        for i in 0..NPC_COUNT {
            npcs.push(NPC { x: (i % 1000) as f64, y: (i / 1000) as f64 });
        }
        Self {
            next_player_id: 1,
            players: HashMap::new(),
            connection_to_player: HashMap::new(),
            npcs,
        }
    }

    fn apply_system(&mut self, command: &GameCommand) {
        match command.kind {
            CommandKind::SystemConnected => {
                if self.connection_to_player.contains_key(&command.connection_id) {
                    return;
                }
                let player_id = self.next_player_id;
                self.next_player_id += 1;
                let spawn_index = ((player_id - 1) % 8) as f64;
                let player = Player {
                    id: player_id,
                    connection_id: command.connection_id,
                    x: 120.0 + spawn_index * 60.0,
                    y: 120.0 + spawn_index * 32.0,
                    target_x: 120.0 + spawn_index * 60.0,
                    target_y: 120.0 + spawn_index * 32.0,
                    aim_x: 1.0,
                    aim_y: 0.0,
                    alive: true,
                    last_processed_seq: 0,
                };
                self.players.insert(player_id, player);
                self.connection_to_player.insert(command.connection_id, player_id);
            }
            CommandKind::SystemDisconnected => {
                if let Some(player_id) = self.connection_to_player.remove(&command.connection_id) {
                    self.players.remove(&player_id);
                }
            }
            _ => {}
        }
    }

    fn apply_gameplay(&mut self, command: &GameCommand) {
        let player_id = match self.connection_to_player.get(&command.connection_id) {
            Some(id) => *id,
            None => return,
        };
        let player = match self.players.get_mut(&player_id) {
            Some(p) => p,
            None => return,
        };

        if !player.alive || command.client_seq <= player.last_processed_seq {
            return;
        }
        player.last_processed_seq = command.client_seq;

        match command.kind {
            CommandKind::MoveTo => {
                player.target_x = command.x.clamp(0.0, WORLD_WIDTH);
                player.target_y = command.y.clamp(0.0, WORLD_HEIGHT);
            }
            CommandKind::Aim => {
                let length = (command.x * command.x + command.y * command.y).sqrt();
                if length > 0.0 {
                    player.aim_x = command.x / length;
                    player.aim_y = command.y / length;
                }
            }
            _ => {}
        }
    }

    fn simulate(&mut self, dt: f64, current_tick: u64) {
        let step = PLAYER_SPEED * dt;
        for player in self.players.values_mut() {
            let dx = player.target_x - player.x;
            let dy = player.target_y - player.y;
            let distance = (dx * dx + dy * dy).sqrt();
            if distance == 0.0 {
                continue;
            }
            if distance <= step {
                player.x = player.target_x;
                player.y = player.target_y;
            } else {
                player.x += dx / distance * step;
                player.y += dy / distance * step;
            }
        }

        // Intensive NPC simulation
        let tick_f = current_tick as f64;
        for (i, npc) in self.npcs.iter_mut().enumerate() {
            let i_f = i as f64;
            npc.x += (tick_f * 0.01 + i_f).sin() * 0.1;
            npc.y += (tick_f * 0.01 + i_f).cos() * 0.1;
        }
    }

    fn player_views(&self) -> Vec<PlayerView> {
        let mut views: Vec<_> = self.players.values().map(|p| PlayerView {
            id: p.id,
            connection_id: p.connection_id,
            x: p.x,
            y: p.y,
            aim_x: p.aim_x,
            aim_y: p.aim_y,
            alive: p.alive,
        }).collect();
        views.sort_by_key(|v| v.id);
        views
    }
}

struct Server {
    args: Args,
    next_connection_id: AtomicU64,
    current_tick: AtomicU64,
    command_tx: mpsc::Sender<GameCommand>,
    connections: Arc<RwLock<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let (command_tx, mut command_rx) = mpsc::channel(args.queue_size);
    let connections = Arc::new(RwLock::new(HashMap::new()));

    let server = Arc::new(Server {
        args: args.clone(),
        next_connection_id: AtomicU64::new(1),
        current_tick: AtomicU64::new(0),
        command_tx,
        connections: connections.clone(),
    });

    let enemy_bases = vec![
        EnemyBaseView { id: 1, x: 180.0, y: 160.0, level: 2, name: "Stone Reef".to_string() },
        EnemyBaseView { id: 2, x: 420.0, y: 260.0, level: 4, name: "Iron Cove".to_string() },
        EnemyBaseView { id: 3, x: 660.0, y: 180.0, level: 6, name: "Storm Pier".to_string() },
        EnemyBaseView { id: 4, x: 540.0, y: 420.0, level: 8, name: "Crab Harbor".to_string() },
    ];

    // Simulation Loop
    let server_sim = server.clone();
    tokio::spawn(async move {
        let mut world = World::new();
        let tick_duration = Duration::from_secs_f64(1.0 / server_sim.args.tick_rate as f64);
        let mut ticker = time::interval(tick_duration);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        
        let broadcast_every = if server_sim.args.broadcast_hz > 0 {
            (server_sim.args.tick_rate / server_sim.args.broadcast_hz).max(1)
        } else {
            0
        };

        let mut future_commands: Vec<GameCommand> = Vec::new();

        loop {
            ticker.tick().await;
            let tick = server_sim.current_tick.fetch_add(1, Ordering::SeqCst);

            // Drain commands
            let mut commands = Vec::new();
            while let Ok(cmd) = command_rx.try_recv() {
                commands.push(cmd);
            }

            // Handle future buffer
            let mut ready = Vec::new();
            let now = tick;
            
            // Re-check future commands
            future_commands.retain(|cmd| {
                if cmd.target_tick <= now {
                    ready.push(cmd.clone());
                    false
                } else {
                    true
                }
            });

            for mut cmd in commands {
                if matches!(cmd.kind, CommandKind::SystemConnected | CommandKind::SystemDisconnected) {
                    cmd.target_tick = tick;
                } else {
                    cmd.target_tick = tick + server_sim.args.input_delay_ticks;
                }

                if cmd.target_tick <= now {
                    ready.push(cmd);
                } else {
                    future_commands.push(cmd);
                }
            }

            // Sort and apply
            ready.sort_by_key(|c| c.target_tick);
            for cmd in ready {
                match cmd.kind {
                    CommandKind::SystemConnected | CommandKind::SystemDisconnected => world.apply_system(&cmd),
                    _ => world.apply_gameplay(&cmd),
                }
            }

            // Keep the CPU-heavy simulation step off the async worker pool.
            world = tokio::task::spawn_blocking(move || {
                let mut world = world;
                world.simulate(tick_duration.as_secs_f64(), tick);
                world
            })
            .await
            .expect("simulation task panicked");

            // Broadcast
            if broadcast_every > 0 && tick % broadcast_every == 0 {
                let snapshot = WorldSnapshot {
                    kind: MessageKind::WorldSnapshot,
                    tick,
                    players: world.player_views(),
                };
                if let Ok(mut data) = serde_json::to_vec(&snapshot) {
                    data.push(b'\n');
                    let conns = server_sim.connections.read().await;
                    for tx in conns.values() {
                        let _ = tx.try_send(data.clone());
                    }
                }
            }
        }
    });

    let listener = TcpListener::bind(&args.addr).await?;
    println!("rust server listening on {} tick_rate={} broadcast_hz={}", args.addr, args.tick_rate, args.broadcast_hz);

    loop {
        let (socket, addr) = listener.accept().await?;
        let server = server.clone();
        let enemy_bases = enemy_bases.clone();

        tokio::spawn(async move {
            let conn_id = server.next_connection_id.fetch_add(1, Ordering::SeqCst);
            let (tx, mut rx) = mpsc::channel(server.args.write_queue_size);
            
            {
                let mut conns = server.connections.write().await;
                conns.insert(conn_id, tx);
            }

            let _ = server.command_tx.send(GameCommand {
                connection_id: conn_id,
                client_seq: 0,
                recv_tick: server.current_tick.load(Ordering::SeqCst),
                target_tick: 0,
                kind: CommandKind::SystemConnected,
                x: 0.0, y: 0.0, item_id: None, product_id: None,
            }).await;

            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            let server_read = server.clone();
            let command_tx = server.command_tx.clone();
            
            let read_task = tokio::spawn(async move {
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let envelope: Envelope = match serde_json::from_str(&line) {
                                Ok(e) => e,
                                Err(_) => continue,
                            };

                            match envelope.kind {
                                MessageKind::GetWorldMap => {
                                    let res = WorldMapResponse {
                                        kind: MessageKind::WorldMap,
                                        seq: envelope.seq.unwrap_or(0),
                                        bases: enemy_bases.clone(),
                                    };
                                    if let Ok(mut data) = serde_json::to_vec(&res) {
                                        data.push(b'\n');
                                        let conns = server_read.connections.read().await;
                                        if let Some(tx) = conns.get(&conn_id) {
                                            let _ = tx.try_send(data);
                                        }
                                    }
                                }
                                MessageKind::MoveTo => {
                                    if let Ok(req) = serde_json::from_str::<MoveToRequest>(&line) {
                                        let _ = command_tx.send(GameCommand {
                                            connection_id: conn_id,
                                            client_seq: req.client_seq,
                                            recv_tick: server_read.current_tick.load(Ordering::SeqCst),
                                            target_tick: 0,
                                            kind: CommandKind::MoveTo,
                                            x: req.x, y: req.y, item_id: None, product_id: None,
                                        }).await;
                                    }
                                }
                                MessageKind::Aim => {
                                    if let Ok(req) = serde_json::from_str::<AimRequest>(&line) {
                                        let _ = command_tx.send(GameCommand {
                                            connection_id: conn_id,
                                            client_seq: req.client_seq,
                                            recv_tick: server_read.current_tick.load(Ordering::SeqCst),
                                            target_tick: 0,
                                            kind: CommandKind::Aim,
                                            x: req.x, y: req.y, item_id: None, product_id: None,
                                        }).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            let write_task = tokio::spawn(async move {
                while let Some(data) = rx.recv().await {
                    if writer.write_all(&data).await.is_err() {
                        break;
                    }
                }
            });

            tokio::select! {
                _ = read_task => {},
                _ = write_task => {},
            }

            {
                let mut conns = server.connections.write().await;
                conns.remove(&conn_id);
            }

            let _ = server.command_tx.send(GameCommand {
                connection_id: conn_id,
                client_seq: 0,
                recv_tick: server.current_tick.load(Ordering::SeqCst),
                target_tick: 0,
                kind: CommandKind::SystemDisconnected,
                x: 0.0, y: 0.0, item_id: None, product_id: None,
            }).await;
        });
    }
}
