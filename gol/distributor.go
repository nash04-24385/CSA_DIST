package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// distributor divides the work between workers and interacts with the broker
func distributor(p Params, c distributorChannels, keypress <-chan rune) {
	// 1. Read input world as before
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioCommand <- ioInput
	c.ioFilename <- filename

	world := make([][]byte, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]byte, p.ImageWidth)
		for x := 0; x < p.ImageWidth; x++ {x
			world[y][x] = <-c.ioInput
		}
	}

	// 2. Connect to broker
	brokerAddr := os.Getenv("BROKER_ADDRESS")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}
	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		return
	}
	defer client.Close()

	// 3. Ticker to fetch alive counts
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan bool)
	turn := 0

	go func() {
		for {
			select {
			case <-ticker.C:
				var aliveCount int
				err := client.Call("Broker.GetAliveCount", struct{}{}, &aliveCount)
				if err == nil {
					c.events <- AliveCellsCount{
						CompletedTurns: turn,
						CellsCount:     aliveCount,
					}
				}
			case <-done:
				return
			}
		}
	}()

	//4. Initial events
	initialAlive := AliveCells(world, p.ImageWidth, p.ImageHeight)
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{CompletedTurns: 0, Cells: initialAlive}
	}
	c.events <- StateChange{turn, Executing}

	// 5. Start Halo Exchange simulation
	fmt.Println("Starting simulation via broker...")
	err = client.Call("Broker.StartSimulation", p, nil)
	if err != nil {
		fmt.Println("Error calling StartSimulation:", err)
		return
	}

	// workers now run autonomously on AWS (halo exchange between each other)
	// we can wait for user input or periodically poll until simulation done

	// 6. Wait for simulation to complete or handle user inputs
	paused := false
	for {
		select {
		case key := <-keypress:
			switch key {
			case 'p':
				if !paused {
					paused = true
					fmt.Println("Paused.")
					c.events <- StateChange{turn, Paused}
				} else {
					paused = false
					fmt.Println("Resumed.")
					c.events <- StateChange{turn, Executing}
				}
			case 's':
				saveImage(p, c, world, turn)
			case 'q':
				saveImage(p, c, world, turn)
				client.Call("Broker.ControllerExit", Empty{}, &Empty{})
				done <- true
				ticker.Stop()
				c.events <- StateChange{turn, Quitting}
				fmt.Println("Quitting...")
				close(c.events)
				return
			case 'k':
				saveImage(p, c, world, turn)
				client.Call("Broker.KillWorkers", Empty{}, &Empty{})
				done <- true
				ticker.Stop()
				c.events <- StateChange{turn, Quitting}
				close(c.events)
				fmt.Println("Full system shutdown.")
				os.Exit(0)
			}
		default:
			// We can add polling here if desired
			time.Sleep(1 * time.Second)
		}
	}

	
}

// HELPER FUNCTIONS :

func AliveCells(world [][]byte, width, height int) []util.Cell {
	cells := make([]util.Cell, 0)
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			if world[dy][dx] == 255 {
				cells = append(cells, util.Cell{X: dx, Y: dy})
			}
		}
	}
	return cells
}

func saveImage(p Params, c distributorChannels, world [][]byte, turn int) {
	outFileName := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioCommand <- ioOutput
	c.ioFilename <- outFileName

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[y][x]
		}
	}

	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: outFileName}
}

