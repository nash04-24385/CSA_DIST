package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

// ------------------------------------------------------------
// Worker struct and setup
// ------------------------------------------------------------

type GOLWorker struct {
	Neigh   gol.Neighbours // up/down neighbour info
	Section [][]byte       // worker’s own part of the world
	StartY  int            // start index (for reconstruction)
	EndY    int
	Params  gol.Params
}

// ------------------------------------------------------------
// RPC: Set neighbour information (called by broker)
// ------------------------------------------------------------

func (w *GOLWorker) SetupNeighbours(neigh gol.Neighbours, _ *struct{}) error {
	w.Neigh = neigh
	fmt.Println("Neighbours set → Up:", neigh.UpAddr, " Down:", neigh.DownAddr)
	return nil
}

// ------------------------------------------------------------
// RPC: Start simulation (called by broker)
// ------------------------------------------------------------

func (w *GOLWorker) StartSimulation(p gol.Params, _ *struct{}) error {
	w.Params = p

	fmt.Printf("Worker starting Halo Exchange simulation (%d turns)...\n", p.Turns)

	go w.runHaloSimulation()

	return nil
}

// ------------------------------------------------------------
// Halo Exchange core logic
// ------------------------------------------------------------

type HaloMsg struct {
	Row []byte
}

// Sends top/bottom rows to neighbours and receives halos
func (w *GOLWorker) exchangeHalos(topRow, bottomRow []byte) ([]byte, []byte) {
	upHaloCh := make(chan []byte, 1)
	downHaloCh := make(chan []byte, 1)

	// Send to UP neighbour, receive bottom halo from it
	go func() {
		conn, err := net.Dial("tcp", w.Neigh.UpAddr)
		if err != nil {
			fmt.Println("Dial error (up):", err)
			upHaloCh <- make([]byte, len(topRow))
			return
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		dec := gob.NewDecoder(conn)

		_ = enc.Encode(HaloMsg{Row: topRow})
		var reply HaloMsg
		_ = dec.Decode(&reply)
		upHaloCh <- reply.Row
	}()

	// Send to DOWN neighbour, receive top halo from it
	go func() {
		conn, err := net.Dial("tcp", w.Neigh.DownAddr)
		if err != nil {
			fmt.Println("Dial error (down):", err)
			downHaloCh <- make([]byte, len(bottomRow))
			return
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		dec := gob.NewDecoder(conn)

		_ = enc.Encode(HaloMsg{Row: bottomRow})
		var reply HaloMsg
		_ = dec.Decode(&reply)
		downHaloCh <- reply.Row
	}()

	upHalo := <-upHaloCh
	downHalo := <-downHaloCh
	return upHalo, downHalo
}

// ------------------------------------------------------------
// Main simulation loop
// ------------------------------------------------------------

func (w *GOLWorker) runHaloSimulation() {
	h := w.Params.ImageHeight
	wid := w.Params.ImageWidth
	turns := w.Params.Turns

	for turn := 0; turn < turns; turn++ {
		topRow := w.Section[0]
		bottomRow := w.Section[len(w.Section)-1]

		// Exchange halos with neighbours
		upHalo, downHalo := w.exchangeHalos(topRow, bottomRow)

		// Compute new state with halos
		newSection := w.calculateNextStatesWithHalo(h, wid, upHalo, downHalo)

		w.Section = newSection

		if turn%50 == 0 || turn == turns-1 {
			fmt.Printf("Worker (%d-%d) completed turn %d\n", w.StartY, w.EndY, turn)
		}
		time.Sleep(1 * time.Millisecond) // optional throttle for debugging
	}

	fmt.Printf("Worker (%d-%d) finished all %d turns.\n", w.StartY, w.EndY, turns)
}

// ------------------------------------------------------------
// Compute next state using halo rows
// ------------------------------------------------------------

func (w *GOLWorker) calculateNextStatesWithHalo(h, wdt int, upHalo, downHalo []byte) [][]byte {
	rows := len(w.Section)
	newRows := make([][]byte, rows)

	for i := 0; i < rows; i++ {
		newRows[i] = make([]byte, wdt)
		for j := 0; j < wdt; j++ {
			count := 0

			upIndex := i - 1
			downIndex := i + 1

			// get neighbour rows safely (using halos if on boundary)
			var upRow, downRow []byte
			if upIndex < 0 {
				upRow = upHalo
			} else {
				upRow = w.Section[upIndex]
			}

			if downIndex >= rows {
				downRow = downHalo
			} else {
				downRow = w.Section[downIndex]
			}

			left := (j - 1 + wdt) % wdt
			right := (j + 1) % wdt

			// count 8 neighbours
			neighbours := []byte{
				upRow[left], upRow[j], upRow[right],
				w.Section[i][left], w.Section[i][right],
				downRow[left], downRow[j], downRow[right],
			}

			for _, cell := range neighbours {
				if cell == 255 {
					count++
				}
			}

			// update cell
			if w.Section[i][j] == 255 {
				if count == 2 || count == 3 {
					newRows[i][j] = 255
				} else {
					newRows[i][j] = 0
				}
			} else {
				if count == 3 {
					newRows[i][j] = 255
				} else {
					newRows[i][j] = 0
				}
			}
		}
	}
	return newRows
}

// ------------------------------------------------------------
// RPC: Return final section (for broker collection)
// ------------------------------------------------------------

func (w *GOLWorker) GetSection(_ struct{}, out *[][]byte) error {
	*out = w.Section
	return nil
}

// ------------------------------------------------------------
// RPC: Shutdown (unchanged)
// ------------------------------------------------------------

func (w *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("Shutdown signal received. Exiting worker...")
	go func() {
		os.Exit(0)
	}()
	return nil
}

// ------------------------------------------------------------
// Main (unchanged except log)
// ------------------------------------------------------------

func main() {
	err := rpc.RegisterName("GOLWorker", new(GOLWorker))
	if err != nil {
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
