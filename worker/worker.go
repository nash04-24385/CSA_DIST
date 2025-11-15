package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

type GOLWorker struct {
	mu sync.RWMutex

	// persistent configuration
	params gol.Params

	// global row range this worker owns: [startY, endY)
	startY int
	endY   int

	// local slice of the world (height = endY-startY, width = params.ImageWidth)
	localWorld [][]byte

	// neighbour worker addresses ("ip:port"), empty string if no neighbour
	aboveAddr string
	belowAddr string
}

// -----------------------------------------------------------------------------
// Baseline method (full-world resync) - kept for compatibility
// -----------------------------------------------------------------------------

// ProcessSection is the old, non-halo version: broker sends full world, worker
// computes [startY,endY) using the global world.
func (w *GOLWorker) ProcessSection(req gol.SectionRequest, res *gol.SectionResponse) error {
	p := req.Params
	world := req.World
	startY := req.StartY
	endY := req.EndY

	updatedSection := calculateNextStatesFullWorld(p, world, startY, endY)

	res.StartY = startY
	res.Section = updatedSection
	return nil
}

// -----------------------------------------------------------------------------
// Initialisation for halo-exchange mode
// -----------------------------------------------------------------------------

// InitSection is called once by the broker to give this worker its slice
// and neighbour addresses.
func (w *GOLWorker) InitSection(req gol.WorkerInitRequest, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.params = req.Params
	w.startY = req.StartY
	w.endY = req.EndY

	// Make a deep copy of the local slice so we own it
	h := req.EndY - req.StartY
	w.localWorld = make([][]byte, h)
	for i := 0; i < h; i++ {
		w.localWorld[i] = make([]byte, w.params.ImageWidth)
		copy(w.localWorld[i], req.LocalWorld[i])
	}

	w.aboveAddr = req.AboveAddr
	w.belowAddr = req.BelowAddr

	fmt.Printf("Worker [%d,%d) initialised. Above=%q Below=%q\n",
		w.startY, w.endY, w.aboveAddr, w.belowAddr)

	return nil
}

// -----------------------------------------------------------------------------
// New: boundary getter RPCs (neighbours PULL these)
// -----------------------------------------------------------------------------

// GetTopBoundary returns a copy of this worker's top local row.
func (w *GOLWorker) GetTopBoundary(_ struct{}, res *gol.HaloRow) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.localWorld) == 0 {
		res.Row = nil
		return nil
	}

	width := w.params.ImageWidth
	row := make([]byte, width)
	copy(row, w.localWorld[0])
	res.Row = row
	return nil
}

// GetBottomBoundary returns a copy of this worker's bottom local row.
func (w *GOLWorker) GetBottomBoundary(_ struct{}, res *gol.HaloRow) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.localWorld) == 0 {
		res.Row = nil
		return nil
	}

	width := w.params.ImageWidth
	h := len(w.localWorld)
	row := make([]byte, width)
	copy(row, w.localWorld[h-1])
	res.Row = row
	return nil
}

// -----------------------------------------------------------------------------
// New: Single "Step" for one Game of Life iteration using PULL halos
// -----------------------------------------------------------------------------

// Step performs one GoL iteration:
//  1. Pull boundary rows from neighbours via GetTop/BottomBoundary.
//  2. Compute next local slice using localWorld + pulled halos.
//  3. Return updated slice to broker.
func (w *GOLWorker) Step(_ struct{}, res *gol.SectionResponse) error {
	// 1. Snapshot parameters under read lock
	w.mu.RLock()
	if w.localWorld == nil {
		w.mu.RUnlock()
		return fmt.Errorf("Step called before InitSection")
	}

	height := len(w.localWorld)
	width := w.params.ImageWidth

	aboveAddr := w.aboveAddr
	belowAddr := w.belowAddr
	startY := w.startY
	params := w.params

	w.mu.RUnlock()

	_ = height // only used conceptually; not needed directly here

	// 2. Pull halos from neighbours
	var topHalo, bottomHalo []byte

	// Get bottom boundary of worker above (wrap-around neighbour)
	if aboveAddr != "" {
		client, err := rpc.Dial("tcp", aboveAddr)
		if err != nil {
			fmt.Println("Step: failed to dial above neighbour:", err)
			return err
		}
		var reply gol.HaloRow
		if err := client.Call("GOLWorker.GetBottomBoundary", struct{}{}, &reply); err != nil {
			fmt.Println("Step: GetBottomBoundary RPC failed:", err)
			client.Close()
			return err
		}
		client.Close()
		topHalo = reply.Row
	}

	// Get top boundary of worker below
	if belowAddr != "" {
		client, err := rpc.Dial("tcp", belowAddr)
		if err != nil {
			fmt.Println("Step: failed to dial below neighbour:", err)
			return err
		}
		var reply gol.HaloRow
		if err := client.Call("GOLWorker.GetTopBoundary", struct{}{}, &reply); err != nil {
			fmt.Println("Step: GetTopBoundary RPC failed:", err)
			client.Close()
			return err
		}
		client.Close()
		bottomHalo = reply.Row
	}

	// 3. Compute next local slice using halos
	w.mu.Lock()
	newLocal := calculateNextUsingHalos(params, w.localWorld, topHalo, bottomHalo)
	w.localWorld = newLocal

	// prepare response for broker
	res.StartY = startY
	res.Section = make([][]byte, len(newLocal))
	for i := range newLocal {
		res.Section[i] = make([]byte, width)
		copy(res.Section[i], newLocal[i])
	}

	w.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// GoL logic
// -----------------------------------------------------------------------------

// Baseline helper: uses full world (your original implementation)
func calculateNextStatesFullWorld(p gol.Params, world [][]byte, startY, endY int) [][]byte {
	h := p.ImageHeight
	w := p.ImageWidth
	rows := endY - startY

	newRows := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		newRows[i] = make([]byte, w)
	}

	for i := startY; i < endY; i++ {
		for j := 0; j < w; j++ {
			count := 0
			up := (i - 1 + h) % h
			down := (i + 1) % h
			left := (j - 1 + w) % w
			right := (j + 1) % w

			if world[i][left] == 255 {
				count++
			}
			if world[i][right] == 255 {
				count++
			}
			if world[up][j] == 255 {
				count++
			}
			if world[down][j] == 255 {
				count++
			}
			if world[up][right] == 255 {
				count++
			}
			if world[up][left] == 255 {
				count++
			}
			if world[down][right] == 255 {
				count++
			}
			if world[down][left] == 255 {
				count++
			}

			if world[i][j] == 255 {
				if count == 2 || count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}
			} else { // world[i][j] == 0
				if count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}
			}
		}
	}
	return newRows
}

// New helper: uses localWorld + halos instead of full world.
// localWorld has shape [hLocal][w], topHalo & bottomHalo are length w.
func calculateNextUsingHalos(p gol.Params, localWorld [][]byte, topHalo, bottomHalo []byte) [][]byte {
	hLocal := len(localWorld)
	if hLocal == 0 {
		return nil
	}
	w := p.ImageWidth

	newLocal := make([][]byte, hLocal)
	for i := 0; i < hLocal; i++ {
		newLocal[i] = make([]byte, w)
	}

	for i := 0; i < hLocal; i++ {
		for j := 0; j < w; j++ {
			count := 0

			// compute indices with wrap in X
			left := (j - 1 + w) % w
			right := (j + 1) % w

			// figure out which row is "up" and "down" in the distributed sense
			var rowUp, rowDown []byte

			if i == 0 {
				rowUp = topHalo
			} else {
				rowUp = localWorld[i-1]
			}

			if i == hLocal-1 {
				rowDown = bottomHalo
			} else {
				rowDown = localWorld[i+1]
			}

			// neighbours:
			if localWorld[i][left] == 255 {
				count++
			}
			if localWorld[i][right] == 255 {
				count++
			}
			if rowUp[j] == 255 {
				count++
			}
			if rowDown[j] == 255 {
				count++
			}
			if rowUp[right] == 255 {
				count++
			}
			if rowUp[left] == 255 {
				count++
			}
			if rowDown[right] == 255 {
				count++
			}
			if rowDown[left] == 255 {
				count++
			}

			if localWorld[i][j] == 255 {
				if count == 2 || count == 3 {
					newLocal[i][j] = 255
				} else {
					newLocal[i][j] = 0
				}
			} else {
				if count == 3 {
					newLocal[i][j] = 255
				} else {
					newLocal[i][j] = 0
				}
			}
		}
	}

	return newLocal
}

// -----------------------------------------------------------------------------
// Shutdown + main
// -----------------------------------------------------------------------------

// helper func to make worker shut down on keypress
func (w *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("shutdown signal received, stopping worker.")
	go func() {
		os.Exit(0)
	}()
	return nil
}

func main() {
	worker := &GOLWorker{}

	if err := rpc.RegisterName("GOLWorker", worker); err != nil {
		fmt.Println("Error registering RPC:", err)
		return
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8030")
	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
		return
	}
	fmt.Println("Worker listening on port 8030 (IPv4)...")
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
