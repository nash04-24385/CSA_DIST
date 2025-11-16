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

	// --- halo exchange state (push + barrier) ---
	topHalo    []byte
	bottomHalo []byte

	halosExpected int // how many halos we expect this turn (0,1,2)
	halosReceived int // how many we’ve actually received

	cond *sync.Cond // condition variable to wait until halosReceived == halosExpected
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

	// allocate halo buffers
	w.topHalo = make([]byte, w.params.ImageWidth)
	w.bottomHalo = make([]byte, w.params.ImageWidth)
	w.halosExpected = 0
	w.halosReceived = 0

	fmt.Printf("Worker [%d,%d) initialised. Above=%q Below=%q\n",
		w.startY, w.endY, w.aboveAddr, w.belowAddr)

	return nil
}

// -----------------------------------------------------------------------------
// Halo RPCs: neighbours PUSH their boundary rows to us
// -----------------------------------------------------------------------------

// SetTopHalo: neighbour sends a row that will be used as the "row above" our localWorld[0]
func (w *GOLWorker) SetTopHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(req.Row) == 0 {
		return nil
	}

	if w.topHalo == nil || len(w.topHalo) != len(req.Row) {
		w.topHalo = make([]byte, len(req.Row))
	}
	copy(w.topHalo, req.Row)

	w.halosReceived++
	if w.cond != nil && w.halosReceived >= w.halosExpected {
		w.cond.Broadcast()
	}
	return nil
}

// SetBottomHalo: neighbour sends a row that will be used as the "row below" our localWorld[hLocal-1]
func (w *GOLWorker) SetBottomHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(req.Row) == 0 {
		return nil
	}

	if w.bottomHalo == nil || len(w.bottomHalo) != len(req.Row) {
		w.bottomHalo = make([]byte, len(req.Row))
	}
	copy(w.bottomHalo, req.Row)

	w.halosReceived++
	if w.cond != nil && w.halosReceived >= w.halosExpected {
		w.cond.Broadcast()
	}
	return nil
}

// -----------------------------------------------------------------------------
// Single "Step" for one Game of Life iteration using PUSH halos + barrier
// -----------------------------------------------------------------------------

// Step performs one GoL iteration with PUSH-based halo exchange:
//   1. Snapshot our top and bottom rows from localWorld (generation t).
//   2. Send those rows to neighbours via SetTopHalo/SetBottomHalo RPCs.
//   3. Wait until both halos have been received (barrier).
//   4. Compute next local slice using localWorld + topHalo + bottomHalo.
//   5. Return updated slice to broker.
func (w *GOLWorker) Step(_ struct{}, res *gol.SectionResponse) error {
	// 1. Snapshot parameters and boundary rows under read lock
	w.mu.RLock()
	if w.localWorld == nil {
		w.mu.RUnlock()
		return fmt.Errorf("Step called before InitSection")
	}

	height := len(w.localWorld)
	width := w.params.ImageWidth
	if height == 0 {
		w.mu.RUnlock()
		return fmt.Errorf("Step called with empty localWorld")
	}

	aboveAddr := w.aboveAddr
	belowAddr := w.belowAddr
	startY := w.startY
	params := w.params

	// copy our boundary rows (gen t)
	topRow := make([]byte, width)
	bottomRow := make([]byte, width)
	copy(topRow, w.localWorld[0])
	copy(bottomRow, w.localWorld[height-1])

	w.mu.RUnlock()

	// 2. Reset halo state for this turn
	w.mu.Lock()

	// ensure halo buffers are allocated
	if w.topHalo == nil || len(w.topHalo) != width {
		w.topHalo = make([]byte, width)
	}
	if w.bottomHalo == nil || len(w.bottomHalo) != width {
		w.bottomHalo = make([]byte, width)
	}

	w.halosExpected = 0
	w.halosReceived = 0

	if aboveAddr != "" {
		w.halosExpected++
	}
	if belowAddr != "" {
		w.halosExpected++
	}

	// if no neighbours (very degenerate), we won't wait
	w.mu.Unlock()

	// 3. Push our boundaries to neighbours (no lock held)

	// send TOP row to the worker ABOVE → they store it as their bottomHalo
	if aboveAddr != "" {
		client, err := rpc.Dial("tcp", aboveAddr)
		if err != nil {
			fmt.Println("Step: failed to dial above neighbour:", err)
			return err
		}
		var reply struct{}
		if err := client.Call("GOLWorker.SetBottomHalo", gol.HaloRow{Row: topRow}, &reply); err != nil {
			fmt.Println("Step: SetBottomHalo RPC failed (above):", err)
			client.Close()
			return err
		}
		client.Close()
	}

	// send BOTTOM row to the worker BELOW → they store it as their topHalo
	if belowAddr != "" {
		client, err := rpc.Dial("tcp", belowAddr)
		if err != nil {
			fmt.Println("Step: failed to dial below neighbour:", err)
			return err
		}
		var reply struct{}
		if err := client.Call("GOLWorker.SetTopHalo", gol.HaloRow{Row: bottomRow}, &reply); err != nil {
			fmt.Println("Step: SetTopHalo RPC failed (below):", err)
			client.Close()
			return err
		}
		client.Close()
	}

	// 4. Wait until we have received all halos for this turn
	w.mu.Lock()
	for w.halosReceived < w.halosExpected {
		if w.cond == nil {
			// should not happen, but avoid deadlock
			break
		}
		w.cond.Wait()
	}

	// compute next local slice using localWorld (gen t) and the halos (also gen t)
	newLocal := calculateNextUsingHalos(params, w.localWorld, w.topHalo, w.bottomHalo)
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
	worker.cond = sync.NewCond(&worker.mu)

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
