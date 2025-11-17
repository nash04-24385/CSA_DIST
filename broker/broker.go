package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

type Broker struct {
	workerAddresses []string
	turn            int
	alive           int

	mu sync.RWMutex

	params      gol.Params
	sections    []section
	initialised bool
}

type section struct {
	start int
	end   int
}

func assignSections(height, workers int) []section {
	minRows := height / workers
	extra := height % workers

	sections := make([]section, workers)
	start := 0

	for i := 0; i < workers; i++ {
		rows := minRows
		if i < extra {
			rows++
		}

		end := start + rows
		sections[i] = section{start, end}
		start = end
	}

	return sections
}

func countAlive(world [][]byte) int {
	c := 0
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				c++
			}
		}
	}
	return c
}

// =====================================================================
// PROCESS SECTION
// =====================================================================

func (broker *Broker) ProcessSection(req gol.BrokerRequest, res *gol.BrokerResponse) error {

	p := req.Params
	numWorkers := len(broker.workerAddresses)

	if numWorkers == 0 {
		return fmt.Errorf("no workers registered")
	}

	// =====================================================================
	// SPECIAL CASE: EXACTLY 1 WORKER → sequential GOL
	// =====================================================================
	if numWorkers == 1 {
		if req.World == nil {
			return fmt.Errorf("missing initial world for sequential mode")
		}

		world := req.World
		H := p.ImageHeight
		W := p.ImageWidth

		next := make([][]byte, H)
		for y := 0; y < H; y++ {
			next[y] = make([]byte, W)
		}

		for y := 0; y < H; y++ {
			up := (y - 1 + H) % H
			down := (y + 1) % H
			for x := 0; x < W; x++ {
				left := (x - 1 + W) % W
				right := (x + 1) % W

				count := 0
				if world[y][left] == 255 {
					count++
				}
				if world[y][right] == 255 {
					count++
				}
				if world[up][x] == 255 {
					count++
				}
				if world[down][x] == 255 {
					count++
				}
				if world[up][left] == 255 {
					count++
				}
				if world[up][right] == 255 {
					count++
				}
				if world[down][left] == 255 {
					count++
				}
				if world[down][right] == 255 {
					count++
				}

				if world[y][x] == 255 {
					if count == 2 || count == 3 {
						next[y][x] = 255
					} else {
						next[y][x] = 0
					}
				} else {
					if count == 3 {
						next[y][x] = 255
					} else {
						next[y][x] = 0
					}
				}
			}
		}

		broker.mu.Lock()
		broker.turn++
		broker.alive = countAlive(next)
		broker.mu.Unlock()

		res.World = next
		return nil
	}

	// =====================================================================
	// MULTI-WORKER CASE (HALO MODE)
	// =====================================================================

	broker.mu.RLock()
	wasInit := broker.initialised
	prev := broker.params
	broker.mu.RUnlock()

	needInit := !wasInit ||
		p.ImageWidth != prev.ImageWidth ||
		p.ImageHeight != prev.ImageHeight ||
		len(broker.sections) != numWorkers

	if needInit {
		initialWorld := req.World
		H := p.ImageHeight

		sections := assignSections(H, numWorkers)

		// map global row -> worker owner
		owner := make([]int, H)
		for i, sec := range sections {
			for r := sec.start; r < sec.end; r++ {
				owner[r] = i
			}
		}

		// SEND INITIAL SLICES
		for i, addr := range broker.workerAddresses {

			sec := sections[i]
			localH := sec.end - sec.start

			localSlice := make([][]byte, localH)
			for r := 0; r < localH; r++ {
				rowIdx := sec.start + r
				localSlice[r] = append([]byte(nil), initialWorld[rowIdx]...)
			}

			// GLOBAL halo rows
			topRowIdx := (sec.start - 1 + H) % H
			botRowIdx := (sec.end) % H

			topInitial := append([]byte(nil), initialWorld[topRowIdx]...)
			bottomInitial := append([]byte(nil), initialWorld[botRowIdx]...)

			// Find the ACTUAL neighbouring workers based on GLOBAL ROW order
			aboveWorker := owner[topRowIdx]
			belowWorker := owner[botRowIdx]

			initReq := gol.WorkerInitRequest{
				Params:        p,
				StartY:        sec.start,
				EndY:          sec.end,
				LocalWorld:    localSlice,
				AboveAddr:     broker.workerAddresses[aboveWorker],
				BelowAddr:     broker.workerAddresses[belowWorker],
				TopInitial:    topInitial,
				BottomInitial: bottomInitial,
			}

			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return err
			}
			client.Call("GOLWorker.InitSection", initReq, nil)
			client.Close()
		}

		broker.mu.Lock()
		broker.sections = sections
		broker.params = p
		broker.initialised = true
		broker.turn = 0
		broker.alive = countAlive(initialWorld)
		broker.mu.Unlock()
	}

	// =====================================================================
	// STEP PHASE
	// =====================================================================

	broker.mu.RLock()
	sections := broker.sections
	params := broker.params
	broker.mu.RUnlock()

	type result struct {
		start int
		rows  [][]byte
		err   error
	}

	ch := make(chan result, numWorkers)

	for i, addr := range broker.workerAddresses {
		sec := sections[i]

		go func(sec section, addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				ch <- result{err: err}
				return
			}
			defer client.Close()

			var q struct{}
			var resp gol.SectionResponse

			if err := client.Call("GOLWorker.Step", q, &resp); err != nil {
				ch <- result{err: err}
				return
			}

			ch <- result{start: resp.StartY, rows: resp.Section}
		}(sec, addr)
	}

	// collect results
	//results := make([]result, numWorkers)
	//for i := 0; i < numWorkers; i++ {
	//	r := <-ch
	//	if r.err != nil {
	//		return r.err
	//	}
	//	results[i] = r
	//}

	results := make([]result, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		r := <-ch
		if r.err != nil {
			return r.err
		}
		results = append(results, r)
	}

	// SORT results by global start row
	sort.Slice(results, func(i, j int) bool {
		return results[i].start < results[j].start
	})

	// stitch world
	newWorld := make([][]byte, params.ImageHeight)
	for _, r := range results {
		for i, row := range r.rows {
			newWorld[r.start+i] = row
		}
	}

	broker.mu.Lock()
	broker.turn++
	broker.alive = countAlive(newWorld)
	broker.mu.Unlock()

	res.World = newWorld
	return nil
}

func (broker *Broker) GetAliveCount(_ struct{}, out *int) error {
	broker.mu.RLock()
	*out = broker.alive
	broker.mu.RUnlock()
	return nil
}

func (broker *Broker) ControllerExit(_ gol.Empty, _ *gol.Empty) error {
	return nil
}

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
			"localhost:8030",
			"localhost:8031",
		},
	}

	if err := rpc.RegisterName("Broker", broker); err != nil {
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
	fmt.Println("Broker listening on port 8040")
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
