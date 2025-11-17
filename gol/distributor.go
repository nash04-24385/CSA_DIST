package gol

import (
	"fmt"
	"net/rpc"
	"os"
)

type distributorChannels struct {
	events     chan<- Event // unused in halo-only version, but Run() requires it
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// --------------------------------------------
// Halo-Only Distributed Controller
// --------------------------------------------
//
// - Loads a PGM via the IO goroutine
// - Sends the world to the Broker
// - For p.Turns iterations, calls Broker.ProcessSection
// - Retrieves the stitched world from the broker each iteration
// - Saves final image
// - Returns final world (for benchmarking or testing)
// --------------------------------------------

func distributor(p Params, c distributorChannels, keypress <-chan rune) {

	// ---------------------------
	// 1. Load initial world from PGM
	// ---------------------------
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioCommand <- ioInput
	c.ioFilename <- filename

	world := make([][]byte, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]byte, p.ImageWidth)
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	// ---------------------------
	// 2. Connect to Broker
	// ---------------------------
	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}

	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		return
	}
	defer client.Close()

	// ---------------------------
	// 3. Distributed halo stepping loop
	// ---------------------------
	for turn := 0; turn < p.Turns; turn++ {

		// prepare request to broker
		request := BrokerRequest{
			Params: p,
			World:  world,
		}

		var response BrokerResponse

		// Step the entire distributed world once
		err := client.Call("Broker.ProcessSection", request, &response)
		if err != nil {
			fmt.Println("Error calling Broker.ProcessSection:", err)
			return
		}

		// replace world with the stitched world returned by the broker
		world = response.World
	}

	// ---------------------------
	// 4. Save final world to PGM
	// ---------------------------
	outputName := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)

	c.ioCommand <- ioOutput
	c.ioFilename <- outputName

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[y][x]
		}
	}

	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// Done
}
