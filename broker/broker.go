package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	"uk.ac.bris.cs/gameoflife/gol"
)

// the broker will keep track of the multiple GOLWorkers
// can use to tell us how many workers we have and then split up the image based on that
type Broker struct {
	workerAddresses []string
	turn            int
	alive           int
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

/* HALO EXCHANGE SETUP */

func (broker *Broker) assignNeighbours() error {
	numWorker := len(broker.workerAddresses)
	for i, addr := range broker.workerAddresses {
		top := broker.workerAddresses[(i-1+numWorker)%numWorker]
		bottom := broker.workerAddresses[(i+1)%numWorker]

		neighbour := gol.Neighbours{UpAddr: top, DownAddr: bottom}

		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, ":", err)
			continue
		}

		err = client.Call("GOLWorker.SetupNeighbours", neighbour, nil)
		if err != nil {
			fmt.Println("Error setting up neighbours for ", addr, ":", err)
		} else {
			fmt.Println("Set neighbours for ", addr, " -> top:", top, " -> bottom:", bottom)
		}
		client.Close()
	}
	return nil
}

/* RPC : Start distributed halo exchange simulation */
func (broker *Broker) startSimulation(p gol.Params, _ *struct{}) error {
	fmt.Println("Starting simulation...")
	for _, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, ":", err)
			continue
		}

		err = client.Call("GOLWorker.StartSimulation", p, nil)
		if err != nil {
			fmt.Println("Error starting simulation on ", addr, ":", err)
		} else {
			fmt.Println("Started simulation on worker: ", addr)
		}
		client.Close()
	}
	return nil
}

/* RPC : Collect final world from all workers */
func (broker *Broker) finalWorld(_ struct{}, world *[][][]byte) error {
	fmt.Println("Finalizing world...")
	var results [][][]byte

	for _, addr := range broker.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to worker", addr, ":", err)
			continue
		}

		var section [][]byte
		err = client.Call("GOLWorker.GetSection", struct{}{}, &section)
		if err == nil {
			results = append(results, section)
			fmt.Println("Collected section from", addr)
		} else {
			fmt.Println("Error getting section from", addr, ":", err)
		}
		client.Close()
	}
	*world = results
	return nil
}

/* Other existing broker rpcs */

func (broker *Broker) GetAliveCount(_ struct{}, out *int) error {
	*out = broker.alive
	return nil
}

func (broker *Broker) ControllerExit(_ gol.Empty, _ *gol.Empty) error {
	fmt.Println("Controller exit requested — broker staying alive.")
	return nil
}

func (broker *Broker) KillWorkers(_ gol.Empty, _ *gol.Empty) error {
	fmt.Println("Kill signal received — shutting down all workers.")
	for _, address := range broker.workerAddresses {
		if c, err := rpc.Dial("tcp", address); err == nil {
			_ = c.Call("GOLWorker.Shutdown", struct{}{}, nil)
			_ = c.Close()
		}
	}
	go os.Exit(0)
	return nil
}

/* Main function */
func main() {
	// ⚠️ Add all your worker instance addresses here:
	broker := &Broker{
		workerAddresses: []string{
			"98.88.28.181:8030",
			"54.221.38.168:8030", // change depending on ip
		},
	}

	// Register broker RPCs
	if err := rpc.RegisterName("Broker", broker); err != nil {
		fmt.Println("Error registering RPC:", err)
		os.Exit(1)
	}

	// Setup Halo Exchange neighbour info
	if err := broker.assignNeighbours(); err != nil {
		fmt.Println("Neighbour assignment failed:", err)
	}

	// Listen for distributor (controller) connections
	listener, err := net.Listen("tcp4", "0.0.0.0:8040")
	if err != nil {
		fmt.Println("Error starting listener:", err)
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
