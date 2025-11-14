package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

// Broker keeps track of all worker instances and coordinates the
// distributed Game of Life simulation.
type Broker struct {
	workerAddresses []string

	mu            sync.Mutex
	finishedCount int
}

// ------------------------------------------------------------
// Helper types / functions used only inside the broker
// ------------------------------------------------------------

// workerSection represents the vertical slice of rows a worker owns:
// rows in [startY, endY] (endY is exclusive).
type workerSection struct {
	startY int
	endY   int
}

// assignSections splits the total image height into roughly-equal
// vertical slices, one per worker.
func assignSections(height, numWorkers int) []workerSection {
	sections := make([]workerSection, numWorkers)

	base := height / numWorkers
	extra := height % numWorkers

	start := 0
	for i := 0; i < numWorkers; i++ {
		rows := base
		if i < extra {
			rows++
		}
		end := start + rows
		sections[i] = workerSection{startY: start, endY: end}
		start = end
	}

	return sections
}

// splitWorld copies the global world into one sub-world per worker
// based on the sections returned by assignSections.
func splitWorld(world [][]byte, sections []workerSection) [][][]byte {
	parts := make([][][]byte, len(sections))
	width := 0
	if len(world) > 0 {
		width = len(world[0])
	}

	for i, s := range sections {
		h := s.endY - s.startY
		sub := make([][]byte, h)
		for row := 0; row < h; row++ {
			sub[row] = make([]byte, width)
			copy(sub[row], world[s.startY+row])
		}
		parts[i] = sub
	}
	return parts
}

// ------------------------------------------------------------
// RPC: setup neighbour relationships between workers
// ------------------------------------------------------------

// assignNeighbours arranges workers in a vertical ring and tells each
// worker who its up/down neighbours are.
func (broker *Broker) assignNeighbours() error {
	numWorkers := len(broker.workerAddresses)
	if numWorkers == 0 {
		return fmt.Errorf("no workers configured on broker")
	}

	for i, addr := range broker.workerAddresses {
		up := broker.workerAddresses[(i-1+numWorkers)%numWorkers]
		down := broker.workerAddresses[(i+1)%numWorkers]

		neigh := gol.Neighbours{
			UpAddr:   up,
			DownAddr: down,
		}

		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, "for neighbour setup:", err)
			continue
		}

		if err := client.Call("GOLWorker.SetupNeighbours", neigh, nil); err != nil {
			fmt.Println("Error calling SetupNeighbours on", addr, ":", err)
		} else {
			fmt.Println("Configured neighbours for worker", addr, "→ up:", up, "down:", down)
		}

		client.Close()
	}

	return nil
}

// ------------------------------------------------------------
// RPC: start the simulation on all workers
// ------------------------------------------------------------

// StartSimulation is invoked by the distributor/controller. It receives
// both the simulation Params and the initial world, splits the world
// into sections, sends each section to a worker, and then starts the
// simulation on each worker. It only returns when all workers report
// that they have finished.
func (broker *Broker) StartSimulation(args gol.SimulationArgs, _ *struct{}) error {
	fmt.Println("Starting simulation from broker...")

	p := args.Params
	world := args.World

	if len(broker.workerAddresses) == 0 {
		return fmt.Errorf("no workers available to start simulation")
	}

	// 1. Split world into vertical sections
	sections := assignSections(p.ImageHeight, len(broker.workerAddresses))
	splitWorlds := splitWorld(world, sections)

	// Reset finished counter
	broker.mu.Lock()
	broker.finishedCount = 0
	broker.mu.Unlock()

	// 2. Send sections and start simulation on each worker
	for i, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, ":", err)
			continue
		}

		// send world section
		if err := client.Call("GOLWorker.ReceiveWorld", splitWorlds[i], nil); err != nil {
			fmt.Println("Error sending world section to", addr, ":", err)
			client.Close()
			continue
		}
		fmt.Println("Sent world section to worker", addr)

		// start simulation
		if err := client.Call("GOLWorker.StartSimulation", p, nil); err != nil {
			fmt.Println("Error starting simulation on", addr, ":", err)
		} else {
			fmt.Println("Started simulation on worker", addr)
		}

		client.Close()
	}

	// 3. Wait until all workers have reported completion
	for {
		broker.mu.Lock()
		done := broker.finishedCount >= len(broker.workerAddresses)
		broker.mu.Unlock()

		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Println("All workers finished simulation.")
	return nil
}

// ------------------------------------------------------------
// RPC: WorkerFinished – called by workers when they are done
// ------------------------------------------------------------

func (broker *Broker) WorkerFinished(_ struct{}, _ *struct{}) error {
	broker.mu.Lock()
	broker.finishedCount++
	count := broker.finishedCount
	broker.mu.Unlock()

	fmt.Println("Worker finished. Total finished:", count)
	return nil
}

// ------------------------------------------------------------
// RPC: GetAliveCount – called periodically by distributor to update UI
// ------------------------------------------------------------

func (broker *Broker) GetAliveCount(_ struct{}, alive *int) error {
	total := 0

	for _, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, "for alive count:", err)
			continue
		}

		var count int
		// Now backed by GOLWorker.GetAliveCount.
		if err := client.Call("GOLWorker.GetAliveCount", struct{}{}, &count); err != nil {
			fmt.Println("Error calling GetAliveCount on", addr, ":", err)
			client.Close()
			continue
		}

		total += count
		client.Close()
	}

	*alive = total
	return nil
}

// ------------------------------------------------------------
// RPC: ControllerExit – called when the local controller quits
// ------------------------------------------------------------

func (broker *Broker) ControllerExit(_ gol.Empty, _ *gol.Empty) error {
	fmt.Println("Controller exited – broker noted.")
	return nil
}

// ------------------------------------------------------------
// RPC: KillWorkers – terminate all workers
// ------------------------------------------------------------

func (broker *Broker) KillWorkers(_ gol.Empty, _ *gol.Empty) error {
	fmt.Println("Killing all workers...")

	for _, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, "for shutdown:", err)
			continue
		}

		// IMPORTANT: reply must be a non-nil pointer for net/rpc.
		var reply struct{}
		if err := client.Call("GOLWorker.Shutdown", struct{}{}, &reply); err != nil {
			fmt.Println("Error shutting down worker", addr, ":", err)
		} else {
			fmt.Println("Shutdown signal sent to worker", addr)
		}

		client.Close()
	}

	return nil
}

// ------------------------------------------------------------
// RPC: CollectFinalWorld – gather final sections from all workers
// ------------------------------------------------------------

// CollectFinalWorld asks each worker for its final world section and
// returns a slice of sections for the distributor to merge.
func (broker *Broker) CollectFinalWorld(_ struct{}, world *[][][]byte) error {
	fmt.Println("Finalizing world...")
	var results [][][]byte

	for _, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, ":", err)
			continue
		}

		var section [][]byte
		if err := client.Call("GOLWorker.GetSection", struct{}{}, &section); err != nil {
			fmt.Println("Error getting section from", addr, ":", err)
			client.Close()
			continue
		}

		results = append(results, section)
		client.Close()
	}

	*world = results
	fmt.Println("Collected", len(results), "sections from workers.")
	return nil
}

// ------------------------------------------------------------
// main – start RPC server
// ------------------------------------------------------------

func main() {
	broker := &Broker{
		workerAddresses: []string{
			// TODO: fill in your worker addresses here, e.g.:
			"172.31.30.142:8030", //DISTCSA
			"172.31.30.21:8030",  //DISTCSA2
		},
	}

	// Configure neighbour relationships once at startup.
	if err := broker.assignNeighbours(); err != nil {
		fmt.Println("Error assigning neighbours:", err)
	}

	if err := rpc.RegisterName("Broker", broker); err != nil {
		fmt.Println("Error registering RPC:", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8040")
	if err != nil {
		fmt.Println("Error starting broker listener:", err)
		os.Exit(1)
	}
	fmt.Println("Broker listening on port 8040 (IPv4)...")

	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
