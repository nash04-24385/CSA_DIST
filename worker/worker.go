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
	mu   sync.RWMutex
	cond *sync.Cond

	params gol.Params

	startY int
	endY   int

	localWorld [][]byte

	aboveAddr string
	belowAddr string

	topHalo    []byte
	bottomHalo []byte

	halosExpected int
	halosReceived int
}

// -----------------------------------------------------------------------------
// INIT (called once)
// -----------------------------------------------------------------------------

func (w *GOLWorker) InitSection(req gol.WorkerInitRequest, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.startY = req.StartY
	w.endY = req.EndY
	w.params = req.Params
	w.aboveAddr = req.AboveAddr
	w.belowAddr = req.BelowAddr

	h := req.EndY - req.StartY
	//wid := req.Params.ImageWidth

	// Deep-copy worker slice
	w.localWorld = make([][]byte, h)
	for i := 0; i < h; i++ {
		w.localWorld[i] = append([]byte(nil), req.LocalWorld[i]...)
	}

	// Allocate halos and copy initial global rows
	w.topHalo = append([]byte(nil), req.TopInitial...)
	w.bottomHalo = append([]byte(nil), req.BottomInitial...)

	if w.cond == nil {
		w.cond = sync.NewCond(&w.mu)
	}

	fmt.Printf("Worker [%d,%d) initialized (above=%s below=%s)\n",
		w.startY, w.endY, w.aboveAddr, w.belowAddr)

	return nil
}

// -----------------------------------------------------------------------------
// Halo RPCs (called by neighbours)
// -----------------------------------------------------------------------------

func (w *GOLWorker) SetTopHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	w.topHalo = append([]byte(nil), req.Row...)
	//copy(w.topHalo, req.Row)
	w.halosReceived++
	w.cond.Broadcast()
	w.mu.Unlock()
	return nil
}

func (w *GOLWorker) SetBottomHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	w.bottomHalo = append([]byte(nil), req.Row...)
	//copy(w.bottomHalo, req.Row)
	w.halosReceived++
	w.cond.Broadcast()
	w.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// STEP (one iteration)
// -----------------------------------------------------------------------------

func (w *GOLWorker) Step(_ struct{}, resp *gol.SectionResponse) error {

	w.mu.Lock()

	if w.localWorld == nil {
		w.mu.Unlock()
		return fmt.Errorf("Step() before InitSection")
	}

	hLocal := len(w.localWorld)
	//wid := w.params.ImageWidth

	// Snapshot boundary rows
	topRow := append([]byte(nil), w.localWorld[0]...)
	bottomRow := append([]byte(nil), w.localWorld[hLocal-1]...)

	// TOROIDAL GRID → always expect 2 halos
	w.halosExpected = 2
	w.halosReceived = 0

	above := w.aboveAddr
	below := w.belowAddr

	w.mu.Unlock()

	// SEND HALOS
	if above != "" {
		if c, err := rpc.Dial("tcp", above); err == nil {
			// I am directly BELOW 'above' worker:
			// my top row is the row just under their last row → their bottom halo
			_ = c.Call("GOLWorker.SetBottomHalo", gol.HaloRow{Row: topRow}, nil)
			c.Close()
		}
	}

	if below != "" {
		if c, err := rpc.Dial("tcp", below); err == nil {
			// I am directly ABOVE 'below' worker:
			// my bottom row is the row just above their first row → their top halo
			_ = c.Call("GOLWorker.SetTopHalo", gol.HaloRow{Row: bottomRow}, nil)
			c.Close()
		}
	}

	// ------------ WAIT FOR BOTH HALOS ------------
	w.mu.Lock()
	for w.halosReceived < w.halosExpected {
		w.cond.Wait()
	}

	// ------------ COMPUTE NEXT LOCAL SLICE ------------
	newLocal := calculateNextUsingHalos(w.params, w.localWorld, w.topHalo, w.bottomHalo)
	w.localWorld = newLocal

	resp.StartY = w.startY
	resp.Section = make([][]byte, hLocal)
	for i := range newLocal {
		resp.Section[i] = append([]byte(nil), newLocal[i]...)
	}

	w.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// GOL Rules (MUST MATCH SEQUENTIAL TESTER EXACTLY)
// -----------------------------------------------------------------------------

func calculateNextUsingHalos(p gol.Params, local [][]byte, top, bottom []byte) [][]byte {
	hLocal := len(local)
	wid := p.ImageWidth

	next := make([][]byte, hLocal)
	for i := 0; i < hLocal; i++ {
		next[i] = make([]byte, wid)
	}

	for i := 0; i < hLocal; i++ {
		for j := 0; j < wid; j++ {

			left := (j - 1 + wid) % wid
			right := (j + 1) % wid

			var upRow, downRow []byte
			if i == 0 {
				upRow = top
			} else {
				upRow = local[i-1]
			}

			if i == hLocal-1 {
				downRow = bottom
			} else {
				downRow = local[i+1]
			}

			count := 0
			if local[i][left] == 255 {
				count++
			}
			if local[i][right] == 255 {
				count++
			}
			if upRow[j] == 255 {
				count++
			}
			if downRow[j] == 255 {
				count++
			}
			if upRow[left] == 255 {
				count++
			}
			if upRow[right] == 255 {
				count++
			}
			if downRow[left] == 255 {
				count++
			}
			if downRow[right] == 255 {
				count++
			}

			if local[i][j] == 255 {
				if count == 2 || count == 3 {
					next[i][j] = 255
				} else {
					next[i][j] = 0
				}
			} else {
				if count == 3 {
					next[i][j] = 255
				} else {
					next[i][j] = 0
				}
			}
		}
	}

	return next
}

// -----------------------------------------------------------------------------
// Shutdown and Main
// -----------------------------------------------------------------------------

func (w *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("Worker shutting down")
	go os.Exit(0)
	return nil
}

func main() {
	worker := &GOLWorker{}
	worker.cond = sync.NewCond(&worker.mu)

	rpc.RegisterName("GOLWorker", worker)

	port := os.Getenv("WORKER_PORT")
	if port == "" {
		port = "8030"
	}
	addr := "0.0.0.0:" + port

	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		fmt.Println("Worker listen error:", err)
		os.Exit(1)
	}
	fmt.Println("Worker listening on", addr)

	for {
		conn, _ := ln.Accept()
		go rpc.ServeConn(conn)
	}
}
