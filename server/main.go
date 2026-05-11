package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultAddress              = "127.0.0.1:43120"
	defaultWorkerCount          = 14
	defaultIOShardCount         = 1
	defaultQueueSize            = 8192
	defaultTickRate             = 60
	defaultBroadcastHz          = 20
	defaultInputDelayTicks      = 2
	defaultMaxStaleTicks        = 20
	defaultMaxFuturePerClient   = 6
	defaultMaxFutureGlobal      = 8192
	defaultMaxCommandsPerTick   = 5
	defaultMaxTotalCommandsTick = 1000
	defaultMaxActionsPerTick    = 256
	defaultWriteQueueSize       = 256
	maxLineBytes                = 128 * 1024
	worldWidth                  = 960
	worldHeight                 = 640
	playerSpeedPerSecond        = 220.0
)

type MessageKind string

const (
	KindUnknown       MessageKind = "Unknown"
	KindGetWorldMap   MessageKind = "Get_World_Map"
	KindWorldMap      MessageKind = "World_Map"
	KindSelectBase    MessageKind = "Select_Base"
	KindError         MessageKind = "Error"
	KindMoveTo        MessageKind = "Move_To"
	KindAim           MessageKind = "Aim"
	KindShoot         MessageKind = "Shoot"
	KindUseItem       MessageKind = "Use_Item"
	KindBuy           MessageKind = "Buy"
	KindWorldSnapshot MessageKind = "World_Snapshot"
)

type CommandKind string

const (
	CommandSystemConnected    CommandKind = "System_Connected"
	CommandSystemDisconnected CommandKind = "System_Disconnected"
	CommandMoveTo             CommandKind = "Move_To"
	CommandAim                CommandKind = "Aim"
	CommandShoot              CommandKind = "Shoot"
	CommandUseItem            CommandKind = "Use_Item"
	CommandBuy                CommandKind = "Buy"
)

type PlayerID uint64

type Envelope struct {
	Kind      MessageKind `json:"kind"`
	Seq       uint64      `json:"seq,omitempty"`
	ClientSeq uint32      `json:"client_seq,omitempty"`
}

type SelectBaseRequest struct {
	Kind   MessageKind `json:"kind"`
	Seq    uint64      `json:"seq"`
	BaseID int         `json:"base_id"`
}

type MoveToRequest struct {
	Kind      MessageKind `json:"kind"`
	Seq       uint64      `json:"seq"`
	ClientSeq uint32      `json:"client_seq"`
	X         float64     `json:"x"`
	Y         float64     `json:"y"`
}

type AimRequest struct {
	Kind      MessageKind `json:"kind"`
	Seq       uint64      `json:"seq"`
	ClientSeq uint32      `json:"client_seq"`
	X         float64     `json:"x"`
	Y         float64     `json:"y"`
}

type ActionRequest struct {
	Kind      MessageKind `json:"kind"`
	Seq       uint64      `json:"seq"`
	ClientSeq uint32      `json:"client_seq"`
	ItemID    string      `json:"item_id,omitempty"`
	ProductID string      `json:"product_id,omitempty"`
}

type EnemyBaseView struct {
	ID    int     `json:"id"`
	X     float32 `json:"x"`
	Y     float32 `json:"y"`
	Level int     `json:"level"`
	Name  string  `json:"name"`
}

type WorldMapResponse struct {
	Kind  MessageKind     `json:"kind"`
	Seq   uint64          `json:"seq"`
	Bases []EnemyBaseView `json:"bases"`
}

type ErrorResponse struct {
	Kind    MessageKind `json:"kind"`
	Seq     uint64      `json:"seq"`
	Message string      `json:"message"`
}

type PlayerView struct {
	ID           PlayerID `json:"id"`
	ConnectionID uint64   `json:"connection_id"`
	X            float64  `json:"x"`
	Y            float64  `json:"y"`
	AimX         float64  `json:"aim_x"`
	AimY         float64  `json:"aim_y"`
	Alive        bool     `json:"alive"`
}

type WorldSnapshot struct {
	Kind    MessageKind  `json:"kind"`
	Tick    uint64       `json:"tick"`
	Players []PlayerView `json:"players"`
}

var enemyBases = []EnemyBaseView{
	{ID: 1, X: 180, Y: 160, Level: 2, Name: "Stone Reef"},
	{ID: 2, X: 420, Y: 260, Level: 4, Name: "Iron Cove"},
	{ID: 3, X: 660, Y: 180, Level: 6, Name: "Storm Pier"},
	{ID: 4, X: 540, Y: 420, Level: 8, Name: "Crab Harbor"},
}

type Options struct {
	Address                 string
	Workers                 int
	IOShards                int
	QueueSize               int
	TickRate                int
	BroadcastHz             int
	InputDelayTicks         uint64
	MaxStaleTicks           uint64
	MaxFuturePerClient      int
	MaxFutureGlobal         int
	MaxCommandsPerTick      int
	MaxTotalCommandsPerTick int
	MaxActionsPerTick       int
	WriteQueueSize          int
}

type Server struct {
	opts         Options
	workQueue    chan WorkItem
	commandQueue chan GameCommand
	shards       []*IOShard

	nextConnectionID atomic.Uint64
	currentTick      atomic.Uint64
	roundRobinShard  atomic.Uint64

	rateLimiter *CommandRateLimiter
	wg          sync.WaitGroup
}

type IOShard struct {
	index       int
	server      *Server
	register    chan *Connection
	unregister  chan *Connection
	responses   chan ResponseItem
	broadcasts  chan BroadcastShardMessage
	connections map[uint64]*Connection
}

type Connection struct {
	ID     uint64
	conn   net.Conn
	shard  *IOShard
	server *Server
	remote string

	send      chan OutboundMessage
	done      chan struct{}
	closed    atomic.Bool
	sendMu    sync.Mutex
	closeOnce sync.Once
}

type WorkItem struct {
	Connection *Connection
	Line       []byte
}

type ResponseItem struct {
	Connection *Connection
	Data       []byte
}

type OutboundMessage struct {
	Data  []byte
	Group *BroadcastGroup
}

type BroadcastShardMessage struct {
	Data []byte
}

type BroadcastGroup struct {
	data        []byte
	remaining   atomic.Int32
	cleanupOnce sync.Once
}

type CommandData struct {
	X         float64
	Y         float64
	ItemID    string
	ProductID string
}

type GameCommand struct {
	ConnectionID uint64
	ClientSeq    uint32
	RecvTick     uint64
	TargetTick   uint64
	Kind         CommandKind
	Data         CommandData
}

type Player struct {
	ID               PlayerID
	ConnectionID     uint64
	X                float64
	Y                float64
	TargetX          float64
	TargetY          float64
	AimX             float64
	AimY             float64
	Alive            bool
	LastProcessedSeq uint32
}

const (
	npcCount = 100000
)

type NPC struct {
	X, Y float64
}

type World struct {
	NextPlayerID       PlayerID
	Players            map[PlayerID]*Player
	ConnectionToPlayer map[uint64]PlayerID
	NPCs               []NPC
}

type FutureBuffer struct {
	commands    []GameCommand
	perClient   map[uint64]int
	perClientN  int
	globalLimit int
}

type CommandRateLimiter struct {
	mu       sync.Mutex
	counters map[uint64]rateCounter
}

type rateCounter struct {
	tick  uint64
	count int
}

type continuousKey struct {
	connectionID uint64
	kind         CommandKind
}

func main() {
	opts := parseOptions()
	server := NewServer(opts)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func parseOptions() Options {
	opts := Options{}
	flag.StringVar(&opts.Address, "addr", defaultAddress, "TCP listen address")
	flag.IntVar(&opts.Workers, "workers", defaultWorkerCount, "request worker goroutines")
	flag.IntVar(&opts.IOShards, "io-shards", defaultIOShardCount, "IO shard goroutines")
	flag.IntVar(&opts.QueueSize, "queue-size", defaultQueueSize, "work and command queue capacity")
	flag.IntVar(&opts.TickRate, "tick-rate", defaultTickRate, "fixed simulation ticks per second")
	flag.IntVar(&opts.BroadcastHz, "broadcast-hz", defaultBroadcastHz, "snapshot broadcasts per second")
	flag.Uint64Var(&opts.InputDelayTicks, "input-delay-ticks", defaultInputDelayTicks, "server-side input delay in ticks")
	flag.Uint64Var(&opts.MaxStaleTicks, "max-stale-ticks", defaultMaxStaleTicks, "drop commands older than this many ticks")
	flag.IntVar(&opts.MaxFuturePerClient, "max-future-per-client", defaultMaxFuturePerClient, "future command cap per connection")
	flag.IntVar(&opts.MaxFutureGlobal, "max-future-global", defaultMaxFutureGlobal, "global future command cap")
	flag.IntVar(&opts.MaxCommandsPerTick, "max-commands-per-tick", defaultMaxCommandsPerTick, "worker-side gameplay command cap per connection per tick")
	flag.IntVar(&opts.MaxTotalCommandsPerTick, "max-total-commands-per-tick", defaultMaxTotalCommandsTick, "simulation-side gameplay command cap per tick")
	flag.IntVar(&opts.MaxActionsPerTick, "max-actions-per-tick", defaultMaxActionsPerTick, "simulation-side discrete action cap per tick")
	flag.IntVar(&opts.WriteQueueSize, "write-queue-size", defaultWriteQueueSize, "per-connection outbound queue capacity")
	flag.Parse()

	if opts.Workers <= 0 || opts.IOShards <= 0 || opts.QueueSize <= 0 || opts.TickRate <= 0 || opts.BroadcastHz < 0 || opts.WriteQueueSize <= 0 {
		log.Fatal("workers, io-shards, queue-size, tick-rate, and write-queue-size must be greater than zero; broadcast-hz must be non-negative")
	}
	if opts.MaxFuturePerClient <= 0 || opts.MaxFutureGlobal <= 0 || opts.MaxCommandsPerTick <= 0 || opts.MaxTotalCommandsPerTick <= 0 || opts.MaxActionsPerTick <= 0 {
		log.Fatal("command caps must be greater than zero")
	}
	if opts.BroadcastHz > opts.TickRate {
		log.Fatal("broadcast-hz must be less than or equal to tick-rate")
	}
	return opts
}

func NewServer(opts Options) *Server {
	server := &Server{
		opts:         opts,
		workQueue:    make(chan WorkItem, opts.QueueSize),
		commandQueue: make(chan GameCommand, opts.QueueSize),
		rateLimiter: &CommandRateLimiter{
			counters: make(map[uint64]rateCounter),
		},
	}
	server.shards = make([]*IOShard, opts.IOShards)
	for i := range server.shards {
		server.shards[i] = &IOShard{
			index:       i,
			server:      server,
			register:    make(chan *Connection, opts.QueueSize),
			unregister:  make(chan *Connection, opts.QueueSize),
			responses:   make(chan ResponseItem, opts.QueueSize),
			broadcasts:  make(chan BroadcastShardMessage, opts.QueueSize),
			connections: make(map[uint64]*Connection),
		}
	}
	return server
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.opts.Address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.opts.Address, err)
	}
	defer listener.Close()

	log.Printf("go server listening on %s io_shards=%d workers=%d tick_rate=%d broadcast_hz=%d", s.opts.Address, s.opts.IOShards, s.opts.Workers, s.opts.TickRate, s.opts.BroadcastHz)

	for _, shard := range s.shards {
		s.wg.Add(1)
		go shard.run(ctx, &s.wg)
	}
	for i := 0; i < s.opts.Workers; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i)
	}
	s.wg.Add(1)
	go s.simulationLoop(ctx)

	acceptDone := make(chan error, 1)
	go func() {
		acceptDone <- s.acceptLoop(ctx, listener)
	}()

	select {
	case <-ctx.Done():
		_ = listener.Close()
	case err := <-acceptDone:
		_ = listener.Close()
		if err != nil && ctx.Err() == nil {
			return err
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop(ctx context.Context, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		connectionID := s.nextConnectionID.Add(1)
		shard := s.nextShard()
		connection := NewConnection(connectionID, conn, shard, s)

		select {
		case shard.register <- connection:
		case <-ctx.Done():
			_ = conn.Close()
			return nil
		}

		s.enqueueSystemCommand(GameCommand{
			ConnectionID: connectionID,
			RecvTick:     s.currentTick.Load(),
			Kind:         CommandSystemConnected,
		})

		s.wg.Add(2)
		go connection.readLoop(ctx, &s.wg)
		go connection.writeLoop(ctx, &s.wg)
	}
}

func (s *Server) nextShard() *IOShard {
	idx := s.roundRobinShard.Add(1) - 1
	return s.shards[int(idx)%len(s.shards)]
}

func NewConnection(id uint64, conn net.Conn, shard *IOShard, server *Server) *Connection {
	return &Connection{
		ID:     id,
		conn:   conn,
		shard:  shard,
		server: server,
		remote: conn.RemoteAddr().String(),
		send:   make(chan OutboundMessage, server.opts.WriteQueueSize),
		done:   make(chan struct{}),
	}
}

func (c *Connection) readLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.Close("read loop ended")

	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 1024), maxLineBytes)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) == 0 {
			continue
		}
		select {
		case c.server.workQueue <- WorkItem{Connection: c, Line: line}:
		case <-ctx.Done():
			return
		default:
			log.Printf("connection %d closed: work queue full", c.ID)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("connection %d read error: %v", c.ID, err)
	}
}

func (c *Connection) writeLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.drainPendingOutbound()
	for {
		select {
		case outbound := <-c.send:
			_, err := c.conn.Write(outbound.Data)
			if outbound.Group != nil {
				outbound.Group.Done()
			}
			if err != nil {
				log.Printf("connection %d write error: %v", c.ID, err)
				c.Close("write failed")
				return
			}
		case <-c.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Connection) Enqueue(outbound OutboundMessage) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if c.closed.Load() {
		if outbound.Group != nil {
			outbound.Group.Done()
		}
		return false
	}
	select {
	case c.send <- outbound:
		return true
	case <-c.done:
		if outbound.Group != nil {
			outbound.Group.Done()
		}
		return false
	default:
		if outbound.Group != nil {
			outbound.Group.Done()
		}
		return false
	}
}

func (c *Connection) Close(reason string) {
	c.closeOnce.Do(func() {
		c.sendMu.Lock()
		c.closed.Store(true)
		close(c.done)
		c.sendMu.Unlock()

		_ = c.conn.Close()
		c.shard.notifyUnregister(c)
		c.server.enqueueSystemCommandAsync(GameCommand{
			ConnectionID: c.ID,
			RecvTick:     c.server.currentTick.Load(),
			Kind:         CommandSystemDisconnected,
		})
		c.server.rateLimiter.Remove(c.ID)
		log.Printf("connection %d closed: %s", c.ID, reason)
	})
}

func (c *Connection) drainPendingOutbound() {
	for {
		select {
		case outbound := <-c.send:
			if outbound.Group != nil {
				outbound.Group.Done()
			}
		default:
			return
		}
	}
}

func (shard *IOShard) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case connection := <-shard.register:
			shard.connections[connection.ID] = connection
			log.Printf("connection %d assigned to io shard %d from %s", connection.ID, shard.index, connection.remote)

		case connection := <-shard.unregister:
			if current := shard.connections[connection.ID]; current == connection {
				delete(shard.connections, connection.ID)
			}

		case response := <-shard.responses:
			if response.Connection == nil || response.Connection.closed.Load() {
				continue
			}
			if !response.Connection.Enqueue(OutboundMessage{Data: response.Data}) {
				response.Connection.Close("response queue full")
			}

		case broadcast := <-shard.broadcasts:
			shard.handleBroadcast(broadcast)

		case <-ctx.Done():
			for _, connection := range shard.connections {
				connection.Close("server stopping")
			}
			return
		}
	}
}

func (shard *IOShard) notifyUnregister(connection *Connection) {
	select {
	case shard.unregister <- connection:
	default:
		go func() { shard.unregister <- connection }()
	}
}

func (shard *IOShard) handleBroadcast(message BroadcastShardMessage) {
	if len(message.Data) == 0 || len(shard.connections) == 0 {
		return
	}

	active := make([]*Connection, 0, len(shard.connections))
	for _, connection := range shard.connections {
		if !connection.closed.Load() {
			active = append(active, connection)
		}
	}
	if len(active) == 0 {
		return
	}

	group := NewBroadcastGroup(message.Data, len(active))
	for _, connection := range active {
		connection.Enqueue(OutboundMessage{Data: message.Data, Group: group})
	}
}

func NewBroadcastGroup(data []byte, scheduledSends int) *BroadcastGroup {
	group := &BroadcastGroup{data: data}
	group.remaining.Store(int32(scheduledSends))
	if scheduledSends == 0 {
		group.cleanup()
	}
	return group
}

func (group *BroadcastGroup) Done() {
	if group.remaining.Add(-1) == 0 {
		group.cleanup()
	}
}

func (group *BroadcastGroup) cleanup() {
	group.cleanupOnce.Do(func() {
		group.data = nil
	})
}

func (s *Server) worker(ctx context.Context, index int) {
	defer s.wg.Done()
	log.Printf("worker %d started", index)
	for {
		select {
		case <-ctx.Done():
			return
		case work := <-s.workQueue:
			s.handleWork(work)
		}
	}
}

func (s *Server) handleWork(work WorkItem) {
	if work.Connection == nil || work.Connection.closed.Load() {
		return
	}

	var envelope Envelope
	if err := json.Unmarshal(work.Line, &envelope); err != nil {
		s.sendError(work.Connection, 0, "invalid JSON message")
		return
	}

	switch envelope.Kind {
	case KindGetWorldMap:
		data, err := encodeJSONLine(WorldMapResponse{Kind: KindWorldMap, Seq: envelope.Seq, Bases: enemyBases})
		if err != nil {
			log.Printf("encode world map response failed: %v", err)
			work.Connection.Close("encode response failed")
			return
		}
		s.sendResponse(work.Connection, data)

	case KindSelectBase:
		var request SelectBaseRequest
		if err := json.Unmarshal(work.Line, &request); err != nil {
			s.sendError(work.Connection, envelope.Seq, "invalid select base request")
			return
		}
		s.sendError(work.Connection, request.Seq, "battle screen is not implemented yet")

	case KindMoveTo:
		s.handleMoveTo(work.Connection, work.Line)

	case KindAim:
		s.handleAim(work.Connection, work.Line)

	case KindShoot, KindUseItem, KindBuy:
		s.handleAction(work.Connection, envelope.Kind, work.Line)

	default:
		s.sendError(work.Connection, envelope.Seq, "unknown message kind")
	}
}

func (s *Server) handleMoveTo(connection *Connection, line []byte) {
	var request MoveToRequest
	if err := json.Unmarshal(line, &request); err != nil {
		s.sendError(connection, 0, "invalid move request")
		return
	}
	if !s.allowGameplayCommand(connection.ID) {
		return
	}
	s.enqueueGameplayCommand(GameCommand{
		ConnectionID: connection.ID,
		ClientSeq:    request.ClientSeq,
		RecvTick:     s.currentTick.Load(),
		Kind:         CommandMoveTo,
		Data: CommandData{
			X: clamp(request.X, 0, worldWidth),
			Y: clamp(request.Y, 0, worldHeight),
		},
	})
}

func (s *Server) handleAim(connection *Connection, line []byte) {
	var request AimRequest
	if err := json.Unmarshal(line, &request); err != nil {
		s.sendError(connection, 0, "invalid aim request")
		return
	}
	if !s.allowGameplayCommand(connection.ID) {
		return
	}
	s.enqueueGameplayCommand(GameCommand{
		ConnectionID: connection.ID,
		ClientSeq:    request.ClientSeq,
		RecvTick:     s.currentTick.Load(),
		Kind:         CommandAim,
		Data: CommandData{
			X: request.X,
			Y: request.Y,
		},
	})
}

func (s *Server) handleAction(connection *Connection, kind MessageKind, line []byte) {
	var request ActionRequest
	if err := json.Unmarshal(line, &request); err != nil {
		s.sendError(connection, 0, "invalid action request")
		return
	}
	if !s.allowGameplayCommand(connection.ID) {
		return
	}

	commandKind := CommandShoot
	switch kind {
	case KindUseItem:
		commandKind = CommandUseItem
	case KindBuy:
		commandKind = CommandBuy
	}

	s.enqueueGameplayCommand(GameCommand{
		ConnectionID: connection.ID,
		ClientSeq:    request.ClientSeq,
		RecvTick:     s.currentTick.Load(),
		Kind:         commandKind,
		Data: CommandData{
			ItemID:    request.ItemID,
			ProductID: request.ProductID,
		},
	})
}

func (s *Server) allowGameplayCommand(connectionID uint64) bool {
	tick := s.currentTick.Load()
	return s.rateLimiter.Allow(connectionID, tick, s.opts.MaxCommandsPerTick)
}

func (s *Server) enqueueGameplayCommand(command GameCommand) {
	select {
	case s.commandQueue <- command:
	default:
		log.Printf("dropping gameplay command from connection %d: command queue full", command.ConnectionID)
	}
}

func (s *Server) enqueueSystemCommand(command GameCommand) {
	s.commandQueue <- command
}

func (s *Server) enqueueSystemCommandAsync(command GameCommand) {
	go s.enqueueSystemCommand(command)
}

func (s *Server) sendError(connection *Connection, seq uint64, message string) {
	data, err := encodeJSONLine(ErrorResponse{Kind: KindError, Seq: seq, Message: message})
	if err != nil {
		log.Printf("encode error response failed: %v", err)
		connection.Close("encode response failed")
		return
	}
	s.sendResponse(connection, data)
}

func (s *Server) sendResponse(connection *Connection, data []byte) {
	select {
	case connection.shard.responses <- ResponseItem{Connection: connection, Data: data}:
	case <-connection.done:
	default:
		connection.Close("response queue full")
	}
}

func (s *Server) simulationLoop(ctx context.Context) {
	defer s.wg.Done()

	tickDuration := time.Second / time.Duration(s.opts.TickRate)
	ticker := time.NewTicker(tickDuration)
	defer ticker.Stop()

	world := NewWorld()
	future := NewFutureBuffer(s.opts.MaxFuturePerClient, s.opts.MaxFutureGlobal)
	broadcastEvery := 0
	if s.opts.BroadcastHz > 0 {
		broadcastEvery = max(1, s.opts.TickRate/s.opts.BroadcastHz)
	}

	var currentTick uint64
	s.currentTick.Store(currentTick)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runSimulationTick(world, future, currentTick, tickDuration)
			if broadcastEvery > 0 && currentTick%uint64(broadcastEvery) == 0 {
				s.broadcastWorld(world, currentTick)
			}
			currentTick++
			s.currentTick.Store(currentTick)
		}
	}
}

func (s *Server) runSimulationTick(world *World, future *FutureBuffer, currentTick uint64, dt time.Duration) {
	batch := s.drainCommandBatch()
	dueFuture := future.PopDue(currentTick)
	if len(dueFuture) > 0 {
		batch = append(batch, dueFuture...)
	}

	ready := make([]GameCommand, 0, len(batch))
	for _, command := range batch {
		command.TargetTick = commandTargetTick(command, s.opts.InputDelayTicks)
		if command.TargetTick > currentTick {
			if !future.Add(command) {
				log.Printf("dropping future command from connection %d: future buffer full", command.ConnectionID)
			}
			continue
		}
		if command.TargetTick+s.opts.MaxStaleTicks < currentTick {
			continue
		}
		ready = append(ready, command)
	}

	systems := make([]GameCommand, 0)
	gameplay := make([]GameCommand, 0)
	for _, command := range ready {
		if isSystemCommand(command.Kind) {
			systems = append(systems, command)
		} else {
			gameplay = append(gameplay, command)
		}
	}

	sort.SliceStable(systems, func(i, j int) bool {
		if systems[i].TargetTick != systems[j].TargetTick {
			return systems[i].TargetTick < systems[j].TargetTick
		}
		if systemPriority(systems[i].Kind) != systemPriority(systems[j].Kind) {
			return systemPriority(systems[i].Kind) < systemPriority(systems[j].Kind)
		}
		return systems[i].ConnectionID < systems[j].ConnectionID
	})
	for _, command := range systems {
		world.ApplySystem(command)
	}

	if len(gameplay) > s.opts.MaxTotalCommandsPerTick {
		log.Printf("dropping %d gameplay commands: max-total-commands-per-tick exceeded", len(gameplay)-s.opts.MaxTotalCommandsPerTick)
		gameplay = gameplay[:s.opts.MaxTotalCommandsPerTick]
	}
	s.applyGameplayCommands(world, gameplay)
	world.Simulate(dt, currentTick)
}

func (s *Server) drainCommandBatch() []GameCommand {
	commands := make([]GameCommand, 0, len(s.commandQueue))
	limit := cap(s.commandQueue)
	for i := 0; i < limit; i++ {
		select {
		case command := <-s.commandQueue:
			commands = append(commands, command)
		default:
			return commands
		}
	}
	return commands
}

func (s *Server) applyGameplayCommands(world *World, commands []GameCommand) {
	if len(commands) == 0 {
		return
	}

	sort.SliceStable(commands, func(i, j int) bool {
		if commands[i].TargetTick != commands[j].TargetTick {
			return commands[i].TargetTick < commands[j].TargetTick
		}
		if commands[i].ConnectionID != commands[j].ConnectionID {
			return commands[i].ConnectionID < commands[j].ConnectionID
		}
		return commands[i].ClientSeq < commands[j].ClientSeq
	})

	latestContinuous := make(map[continuousKey]uint32)
	for _, command := range commands {
		if !isContinuousCommand(command.Kind) {
			continue
		}
		key := continuousKey{connectionID: command.ConnectionID, kind: command.Kind}
		if command.ClientSeq > latestContinuous[key] {
			latestContinuous[key] = command.ClientSeq
		}
	}

	actionsApplied := 0
	for _, command := range commands {
		if isContinuousCommand(command.Kind) {
			key := continuousKey{connectionID: command.ConnectionID, kind: command.Kind}
			if command.ClientSeq != latestContinuous[key] {
				continue
			}
		} else {
			if actionsApplied >= s.opts.MaxActionsPerTick {
				continue
			}
			actionsApplied++
		}
		world.ApplyGameplay(command)
	}
}

func (s *Server) broadcastWorld(world *World, tick uint64) {
	snapshot := WorldSnapshot{
		Kind:    KindWorldSnapshot,
		Tick:    tick,
		Players: world.PlayerViews(),
	}
	data, err := encodeJSONLine(snapshot)
	if err != nil {
		log.Printf("encode snapshot failed: %v", err)
		return
	}
	for _, shard := range s.shards {
		clone := append([]byte(nil), data...)
		select {
		case shard.broadcasts <- BroadcastShardMessage{Data: clone}:
		default:
			log.Printf("dropping broadcast for io shard %d: broadcast queue full", shard.index)
		}
	}
}

func NewWorld() *World {
	npcs := make([]NPC, npcCount)
	for i := 0; i < npcCount; i++ {
		npcs[i] = NPC{X: float64(i % 1000), Y: float64(i / 1000)}
	}
	return &World{
		NextPlayerID:       1,
		Players:            make(map[PlayerID]*Player),
		ConnectionToPlayer: make(map[uint64]PlayerID),
		NPCs:               npcs,
	}
}

func (world *World) ApplySystem(command GameCommand) {
	switch command.Kind {
	case CommandSystemConnected:
		if _, exists := world.ConnectionToPlayer[command.ConnectionID]; exists {
			return
		}
		playerID := world.NextPlayerID
		world.NextPlayerID++
		spawnIndex := float64((playerID - 1) % 8)
		player := &Player{
			ID:           playerID,
			ConnectionID: command.ConnectionID,
			X:            120 + spawnIndex*60,
			Y:            120 + spawnIndex*32,
			TargetX:      120 + spawnIndex*60,
			TargetY:      120 + spawnIndex*32,
			AimX:         1,
			Alive:        true,
		}
		world.Players[playerID] = player
		world.ConnectionToPlayer[command.ConnectionID] = playerID

	case CommandSystemDisconnected:
		playerID, exists := world.ConnectionToPlayer[command.ConnectionID]
		if !exists {
			return
		}
		delete(world.ConnectionToPlayer, command.ConnectionID)
		delete(world.Players, playerID)
	}
}

func (world *World) ApplyGameplay(command GameCommand) {
	player := world.playerForConnection(command.ConnectionID)
	if player == nil || !player.Alive {
		return
	}
	if command.ClientSeq <= player.LastProcessedSeq {
		return
	}
	player.LastProcessedSeq = command.ClientSeq

	switch command.Kind {
	case CommandMoveTo:
		player.TargetX = clamp(command.Data.X, 0, worldWidth)
		player.TargetY = clamp(command.Data.Y, 0, worldHeight)

	case CommandAim:
		length := math.Hypot(command.Data.X, command.Data.Y)
		if length > 0 {
			player.AimX = command.Data.X / length
			player.AimY = command.Data.Y / length
		}

	case CommandShoot:
		// Placeholder for authoritative cooldown, ammo, and range checks.

	case CommandUseItem:
		// Placeholder for authoritative ownership and cooldown checks.

	case CommandBuy:
		// Placeholder for authoritative currency and inventory checks.
	}
}

func (world *World) Simulate(dt time.Duration, currentTick uint64) {
	step := playerSpeedPerSecond * dt.Seconds()
	for _, player := range world.Players {
		dx := player.TargetX - player.X
		dy := player.TargetY - player.Y
		distance := math.Hypot(dx, dy)
		if distance == 0 {
			continue
		}
		if distance <= step {
			player.X = player.TargetX
			player.Y = player.TargetY
			continue
		}
		player.X += dx / distance * step
		player.Y += dy / distance * step
	}

	// Intensive NPC simulation
	tickF := float64(currentTick)
	for i := 0; i < npcCount; i++ {
		world.NPCs[i].X += math.Sin(tickF*0.01+float64(i)) * 0.1
		world.NPCs[i].Y += math.Cos(tickF*0.01+float64(i)) * 0.1
	}
}

func (world *World) PlayerViews() []PlayerView {
	views := make([]PlayerView, 0, len(world.Players))
	for _, player := range world.Players {
		views = append(views, PlayerView{
			ID:           player.ID,
			ConnectionID: player.ConnectionID,
			X:            player.X,
			Y:            player.Y,
			AimX:         player.AimX,
			AimY:         player.AimY,
			Alive:        player.Alive,
		})
	}
	sort.Slice(views, func(i, j int) bool {
		return views[i].ID < views[j].ID
	})
	return views
}

func (world *World) playerForConnection(connectionID uint64) *Player {
	playerID, exists := world.ConnectionToPlayer[connectionID]
	if !exists {
		return nil
	}
	return world.Players[playerID]
}

func NewFutureBuffer(perClientLimit, globalLimit int) *FutureBuffer {
	return &FutureBuffer{
		perClient:   make(map[uint64]int),
		perClientN:  perClientLimit,
		globalLimit: globalLimit,
	}
}

func (buffer *FutureBuffer) Add(command GameCommand) bool {
	if len(buffer.commands) >= buffer.globalLimit {
		return false
	}
	if buffer.perClient[command.ConnectionID] >= buffer.perClientN {
		return false
	}
	buffer.commands = append(buffer.commands, command)
	buffer.perClient[command.ConnectionID]++
	return true
}

func (buffer *FutureBuffer) PopDue(currentTick uint64) []GameCommand {
	if len(buffer.commands) == 0 {
		return nil
	}
	due := make([]GameCommand, 0)
	remaining := buffer.commands[:0]
	for _, command := range buffer.commands {
		if command.TargetTick <= currentTick {
			due = append(due, command)
			buffer.perClient[command.ConnectionID]--
			if buffer.perClient[command.ConnectionID] <= 0 {
				delete(buffer.perClient, command.ConnectionID)
			}
			continue
		}
		remaining = append(remaining, command)
	}
	buffer.commands = remaining
	return due
}

func (limiter *CommandRateLimiter) Allow(connectionID uint64, tick uint64, maxCommands int) bool {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	counter := limiter.counters[connectionID]
	if counter.tick != tick {
		counter.tick = tick
		counter.count = 0
	}
	if counter.count >= maxCommands {
		limiter.counters[connectionID] = counter
		return false
	}
	counter.count++
	limiter.counters[connectionID] = counter
	return true
}

func (limiter *CommandRateLimiter) Remove(connectionID uint64) {
	limiter.mu.Lock()
	delete(limiter.counters, connectionID)
	limiter.mu.Unlock()
}

func commandTargetTick(command GameCommand, inputDelayTicks uint64) uint64 {
	if isSystemCommand(command.Kind) {
		return command.RecvTick
	}
	return command.RecvTick + inputDelayTicks
}

func isSystemCommand(kind CommandKind) bool {
	return kind == CommandSystemConnected || kind == CommandSystemDisconnected
}

func isContinuousCommand(kind CommandKind) bool {
	return kind == CommandMoveTo || kind == CommandAim
}

func systemPriority(kind CommandKind) int {
	switch kind {
	case CommandSystemConnected:
		return 0
	case CommandSystemDisconnected:
		return 1
	default:
		return 2
	}
}

func encodeJSONLine(message any) ([]byte, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	return data, nil
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
