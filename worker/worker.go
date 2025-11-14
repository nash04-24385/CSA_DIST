package main

import (
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
	StartY  int            // optional: for debug
	EndY    int
	Params  gol.Params
}

// This type is used for halo exchange RPCs between workers.
type HaloMsg struct {
	Row []byte
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
// RPC: Receive world section (called by broker)
// ------------------------------------------------------------

func (w *GOLWorker) ReceiveWorld(section [][]byte, _ *struct{}) error {
	if len(section) == 0 {
		fmt.Println("⚠️ Received empty section — skipping.")
		return nil
	}
	w.Section = section
	fmt.Printf("Worker received world section with %d rows\n", len(section))
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
// Halo exchange RPC interface between workers
// ------------------------------------------------------------

// Called by DOWN neighbour to get our TOP row.
func (w *GOLWorker) ExchangeUp(_ HaloMsg, resp *HaloMsg) error {
	if len(w.Section) == 0 {
		resp.Row = nil
		return nil
	}
	resp.Row = w.Section[0]
	return nil
}

// Called by UP neighbour to get our BOTTOM row.
func (w *GOLWorker) ExchangeDown(_ HaloMsg, resp *HaloMsg) error {
	if len(w.Section) == 0 {
		resp.Row = nil
		return nil
	}
	resp.Row = w.Section[len(w.Section)-1]
	return nil
}

// Local helper: talk to neighbours to get halo rows for this turn.
func (w *GOLWorker) exchangeHalos(topRow, bottomRow []byte) ([]byte, []byte) {
	rows := len(w.Section)

	// Special case:
	// - this worker owns the entire height of the world (no vertical split)
	// - neighbours are the same (ring of size 1)
	// then halos are just our own bottom/top rows (toroidal wrap), no RPC needed.
	if rows == w.Params.ImageHeight && (w.Neigh.UpAddr == "" || w.Neigh.UpAddr == w.Neigh.DownAddr) {
		return bottomRow, topRow
	}

	upHaloCh := make(chan []byte, 1)
	downHaloCh := make(chan []byte, 1)

	// Ask UP neighbour for its bottom row
	go func() {
		defer close(upHaloCh)

		client, err := rpc.Dial("tcp", w.Neigh.UpAddr)
		if err != nil {
			fmt.Println("RPC dial error (up):", err)
			upHaloCh <- make([]byte, len(topRow))
			return
		}
		defer func() { _ = client.Close() }()

		req := HaloMsg{Row: bottomRow}
		var resp HaloMsg
		if err := client.Call("GOLWorker.ExchangeDown", req, &resp); err != nil {
			fmt.Println("RPC call error (up):", err)
			upHaloCh <- make([]byte, len(topRow))
			return
		}
		if resp.Row == nil {
			resp.Row = make([]byte, len(topRow))
		}
		upHaloCh <- resp.Row
	}()

	// Ask DOWN neighbour for its top row
	go func() {
		defer close(downHaloCh)

		client, err := rpc.Dial("tcp", w.Neigh.DownAddr)
		if err != nil {
			fmt.Println("RPC dial error (down):", err)
			downHaloCh <- make([]byte, len(bottomRow))
			return
		}
		defer func() { _ = client.Close() }()

		req := HaloMsg{Row: topRow}
		var resp HaloMsg
		if err := client.Call("GOLWorker.ExchangeUp", req, &resp); err != nil {
			fmt.Println("RPC call error (down):", err)
			downHaloCh <- make([]byte, len(bottomRow))
			return
		}
		if resp.Row == nil {
			resp.Row = make([]byte, len(bottomRow))
		}
		downHaloCh <- resp.Row
	}()

	upHalo := <-upHaloCh
	downHalo := <-downHaloCh

	return upHalo, downHalo
}

// ------------------------------------------------------------
// Main simulation loop
// ------------------------------------------------------------

func (w *GOLWorker) runHaloSimulation() {
	if len(w.Section) == 0 || w.Params.ImageWidth == 0 || w.Params.Turns == 0 {
		w.notifyBrokerFinished()
		return
	}

	width := w.Params.ImageWidth
	turns := w.Params.Turns

	for turn := 0; turn < turns; turn++ {
		rows := len(w.Section)
		if rows == 0 {
			break
		}

		topRow := w.Section[0]
		bottomRow := w.Section[rows-1]

		// Exchange halos with neighbours
		upHalo, downHalo := w.exchangeHalos(topRow, bottomRow)

		// Compute new state with halos
		w.Section = w.calculateNextStatesWithHalo(width, upHalo, downHalo)

		if turn%50 == 0 || turn == turns-1 {
			fmt.Printf("Worker (%d-%d) completed turn %d\n", w.StartY, w.EndY, turn)
		}

		// Small sleep is optional; helps debug
		time.Sleep(1 * time.Millisecond)
	}

	fmt.Printf("Worker (%d-%d) finished all %d turns.\n", w.StartY, w.EndY, turns)
	w.notifyBrokerFinished()
}

// ------------------------------------------------------------
// Compute next state using halo rows
// ------------------------------------------------------------

// Compute next state using halo rows from neighbours.
func (w *GOLWorker) calculateNextStatesWithHalo(width int, upHalo, downHalo []byte) [][]byte {
    rows := len(w.Section)
    newRows := make([][]byte, rows)

    for y := 0; y < rows; y++ {
        newRows[y] = make([]byte, width)

        for x := 0; x < width; x++ {
            neighbours := 0

            // check 8 neighbours
            for dy := -1; dy <= 1; dy++ {
                for dx := -1; dx <= 1; dx++ {
                    if dy == 0 && dx == 0 {
                        continue
                    }

                    ny := y + dy
                    var row []byte
                    if ny < 0 {
                        // row above the first -> comes from upHalo
                        row = upHalo
                    } else if ny >= rows {
                        // row below the last -> comes from downHalo
                        row = downHalo
                    } else {
                        row = w.Section[ny]
                    }

                    nx := (x + dx + width) % width // horizontal wrap
                    if row[nx] == 255 {
                        neighbours++
                    }
                }
            }

            // apply GoL rules
            if w.Section[y][x] == 255 {
                if neighbours == 2 || neighbours == 3 {
                    newRows[y][x] = 255
                } else {
                    newRows[y][x] = 0
                }
            } else {
                if neighbours == 3 {
                    newRows[y][x] = 255
                } else {
                    newRows[y][x] = 0
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
// RPC: GetAliveCount – used by broker for stats
// ------------------------------------------------------------

func (w *GOLWorker) GetAliveCount(_ struct{}, out *int) error {
	count := 0
	for _, row := range w.Section {
		for _, cell := range row {
			if cell == 255 {
				count++
			}
		}
	}
	*out = count
	return nil
}

// ------------------------------------------------------------
// RPC: Shutdown
// ------------------------------------------------------------

func (w *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("Shutdown signal received. Exiting worker...")
	go func() {
		os.Exit(0)
	}()
	return nil
}

// ------------------------------------------------------------
// Notify broker that this worker has finished all turns
// ------------------------------------------------------------

func (w *GOLWorker) notifyBrokerFinished() {
	brokerAddr := os.Getenv("BROKER_ADDRESS")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}

	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker to report finish:", err)
		return
	}
	defer client.Close()

	var reply struct{}
	if err := client.Call("Broker.WorkerFinished", struct{}{}, &reply); err != nil {
		fmt.Println("Error calling Broker.WorkerFinished:", err)
	} else {
		fmt.Println("Reported completion to broker.")
	}
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------

func main() {
	if err := rpc.RegisterName("GOLWorker", new(GOLWorker)); err != nil {
		fmt.Println("Error registering RPC:", err)
		os.Exit(1)
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
