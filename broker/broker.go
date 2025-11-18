package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

// the broker will keep track of the multiple GOLWorkers
// can use to tell us how many workers we have and then split up the image based on that
type Broker struct {
	workerAddresses []string
	turn            int
	alive           int

	mu sync.RWMutex

	//Added new fields for the halo-exchange implementation

	params      gol.Params // params stores the last Params used to initialise the workers
	sections    []section  // sections remembers which global row range each worker owns
	initialised bool       // tells us whether we've already sent initial slices to workers
}

type section struct {
	start int
	end   int
}

// assign section helper function from before
// helper func to assign sections of image to workers based on no. of threads
func assignSections(height, workers int) []section {

	// we need to calculate the minimum number of rows for each worker
	minRows := height / workers
	// then say if we have extra rows left over then we need to assign those evenly to each worker
	extraRows := height % workers

	// make a slice, the size of the number of threads
	sections := make([]section, workers)
	start := 0

	for i := 0; i < workers; i++ {
		// assigns the base amount of rows to the thread
		rows := minRows
		// if say we're on worker 2 and there are 3 extra rows left,
		// then we can add 1 more job to the thread
		if i < extraRows {
			rows++
		}

		// marks where the end of the section ends
		end := start + rows
		// assigns these rows to the section
		sections[i] = section{start: start, end: end}
		// start is updated for the next worker
		start = end
	}
	return sections
}

// function to count the number of alive cells
func countAlive(world [][]byte) int {
	count := 0
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				count++
			}
		}
	}

	return count
}

// one iteration of the game using all workers

//ProcessSection is called by the distributor once per turn
/*
In OG implementation, this function re-sent the entire world to every worker on every iteration, and each worker returned an updated slice.
Every worker on every iteration, and each worker returned an updated slice.
This matches the "easy" baseline in the coursework but has heavy communication overhead

In halo exchange version, we change the behaviour:
- On the first call, we split the world into sections and send each worker only its own slice plus the addresses of its neighbour workers
- Workers keep this slice and manage halo rows internally
- On every call (including the first after initialisation), we ask each worker to perform a single step (halo exchange + local update) and return its updated slice
- we then stitch the full world together again for the distributor and/or tests

*/

func (broker *Broker) ProcessSection(req gol.BrokerRequest, res *gol.BrokerResponse) error {
	p := req.Params
	world := req.World

	numWorkers := len(broker.workerAddresses)

	if numWorkers == 0 {
		return fmt.Errorf("no workers registered")
	}

	// Snapshot current params/initialised under read lock so we can decide
	// if we need to (re)initialise workers for this request.
	broker.mu.RLock()
	prevParams := broker.params
	wasInitialised := broker.initialised
	broker.mu.RUnlock()

	needInit := !wasInitialised ||
		p.ImageWidth != prevParams.ImageWidth ||
		p.ImageHeight != prevParams.ImageHeight ||
		p.Turns != prevParams.Turns ||
		p.Threads != prevParams.Threads

	// ---------------------------------------------------------------------
	// (Re)initialise workers if needed
	// ---------------------------------------------------------------------
	if needInit {
		sections := assignSections(p.ImageHeight, numWorkers)

		// Build local slices for each worker *without* holding the lock
		workerLocalWorlds := make([][][]byte, numWorkers)
		for i, sec := range sections {
			localHeight := sec.end - sec.start
			localWorld := make([][]byte, localHeight)
			for row := 0; row < localHeight; row++ {
				localWorld[row] = make([]byte, p.ImageWidth)
				copy(localWorld[row], world[sec.start+row])
			}
			workerLocalWorlds[i] = localWorld
		}

		// Perform InitSection RPCs (this can block, so no lock here)
		for i, address := range broker.workerAddresses {
			sec := sections[i]
			localWorld := workerLocalWorlds[i]

			// ring neighbours (single worker -> self-neighbour)
			var aboveAddr, belowAddr string
			if numWorkers == 1 {
				aboveAddr = address
				belowAddr = address
			} else {
				aboveAddr = broker.workerAddresses[(i-1+numWorkers)%numWorkers]
				belowAddr = broker.workerAddresses[(i+1)%numWorkers]
			}

			initReq := gol.WorkerInitRequest{
				Params:     p,
				StartY:     sec.start,
				EndY:       sec.end,
				LocalWorld: localWorld,
				AboveAddr:  aboveAddr,
				BelowAddr:  belowAddr,
			}

			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return fmt.Errorf("error dialing worker %s for InitSection: %w", address, err)
			}

			var reply struct{}
			if err := client.Call("GOLWorker.InitSection", initReq, &reply); err != nil {
				client.Close()
				return fmt.Errorf("InitSection RPC failed for worker %s: %w", address, err)
			}
			client.Close()
		}

		// 🔐 Only now, once all InitSection RPCs succeeded, update shared state
		broker.mu.Lock()
		broker.params = p
		broker.sections = sections
		broker.initialised = true
		broker.turn = 0
		broker.alive = countAlive(world)
		broker.mu.Unlock()
	}

	// ---------------------------------------------------------------------
	// Step: ask each worker to perform one halo-exchange step
	// ---------------------------------------------------------------------

	// Take a snapshot of sections/params under read lock
	broker.mu.RLock()
	sections := broker.sections
	params := broker.params
	broker.mu.RUnlock()

	type sectionResult struct {
		start int
		rows  [][]byte
		err   error
	}

	resultsChan := make(chan sectionResult, numWorkers)

	for i, address := range broker.workerAddresses {

		sec := sections[i]
		address := address

		go func(sec section, address string) {
			client, err := rpc.Dial("tcp", address)
			if err != nil {

				resultsChan <- sectionResult{err: fmt.Errorf("dial %s for Step: %w", address, err)}
				return
			}

			defer client.Close()

			var stepReq struct{}
			var stepRes gol.SectionResponse

			if err := client.Call("GOLWorker.Step", stepReq, &stepRes); err != nil {
				resultsChan <- sectionResult{err: fmt.Errorf("Step RPC %s: %w", address, err)}
				return
			}

			resultsChan <- sectionResult{

				start: stepRes.StartY,
				rows:  stepRes.Section,
				err:   nil,
			}

		}(sec, address)
	}

	results := make([]sectionResult, numWorkers)
	for i := 0; i < numWorkers; i++ {

		r := <-resultsChan
		if r.err != nil {
			return r.err
		}
		results[i] = r
	}

	close(resultsChan)

	// Stitch full world
	newWorld := make([][]byte, params.ImageHeight)
	for _, r := range results {
		for i, row := range r.rows {
			y := r.start + i
			newWorld[y] = row
		}
	}

	// 🔐 Update turn + alive under lock so GetAliveCount sees a consistent snapshot
	broker.mu.Lock()
	broker.turn++
	broker.alive = countAlive(newWorld)
	broker.mu.Unlock()

	res.World = newWorld
	return nil
}

// allows distributor to request the current count from the broker
// broker has access to the most recent number of alive cells in the world
func (broker *Broker) GetAliveCount(_ struct{}, out *int) error {
	broker.mu.RLock()
	*out = broker.alive
	broker.mu.RUnlock()
	return nil
}

// We need a function that when q (quit) is pressed then the controller
// exit without killing the simulation
// when q is pressed we need to save the current board (pgm), then call a function that
// doesnt persist the world -> basically do nothing
func (broker *Broker) ControllerExit(_ gol.Empty, _ *gol.Empty) error {
	return nil
}

// when k is pressed, we need to call a function that would send GOL.Shutdown
// to each worker and then kill itself
// then the controller saves the final image and exits
func (broker *Broker) KillWorkers(_ gol.Empty, _ *gol.Empty) error {
	for _, address := range broker.workerAddresses {
		if c, err := rpc.Dial("tcp", address); err == nil {
			_ = c.Call("GOLWorker.Shutdown", struct{}{}, nil)
			_ = c.Close()
		}
	}

	go os.Exit(0)

	return nil
}

func main() {

	broker := &Broker{
		workerAddresses: []string{
			// CAN ALWAYS CHANGE
			"127.0.0.1:8030",
		},
	}

	err := rpc.RegisterName("Broker", broker)

	if err != nil {
		fmt.Println("Error registering RPC:", err)
		os.Exit(1)
		return
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8040")

	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
		return
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
