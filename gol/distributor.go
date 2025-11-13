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

// SimulationArgs bundles Params + initial world so the broker
// can start the distributed simulation from the correct state.

// distributor divides the work between workers and interacts with the broker.
func distributor(p Params, c distributorChannels, keypress <-chan rune) {
	// 1. Read initial world from PGM
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

	turn := 0

	// 2. Initial events
	initialAlive := AliveCells(world, p.ImageWidth, p.ImageHeight)
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{CompletedTurns: 0, Cells: initialAlive}
	}
	c.events <- StateChange{turn, Executing}

	// 3. Handle 0-turn tests locally (no broker needed)
	if p.Turns == 0 {
		aliveCells := AliveCells(world, p.ImageWidth, p.ImageHeight)
		c.events <- FinalTurnComplete{CompletedTurns: 0, Alive: aliveCells}
		saveImage(p, c, world, 0)
		c.events <- StateChange{0, Quitting}
		close(c.events)
		return
	}

	// 4. Connect to broker
	brokerAddr := os.Getenv("BROKER_ADDRESS")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}

	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		c.events <- StateChange{turn, Quitting}
		close(c.events)
		return
	}
	defer client.Close()

	// 5. Start distributed simulation on broker in the background
	args := SimulationArgs{
		Params: p,
		World:  world,
	}

	finalWorldCh := make(chan [][]byte, 1)
	simErrCh := make(chan error, 1)

	go func() {
		var startReply struct{}
		if err := client.Call("Broker.StartSimulation", args, &startReply); err != nil {
			simErrCh <- err
			close(finalWorldCh)
			return
		}

		// When simulation is done on the broker, collect the final world
		var sections [][][]byte
		if err := client.Call("Broker.CollectFinalWorld", struct{}{}, &sections); err != nil {
			simErrCh <- err
			close(finalWorldCh)
			return
		}

		finalWorldCh <- mergeSections(sections)
	}()

	// 6. Ticker goroutine to fetch alive counts periodically
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				var aliveCount int
				if err := client.Call("Broker.GetAliveCount", struct{}{}, &aliveCount); err == nil {
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

	// 7. Main loop: handle keypresses and wait for simulation completion
	paused := false

	for {
		select {
		// Simulation finished on broker
		case w, ok := <-finalWorldCh:
			if !ok || w == nil {
				// error already sent on simErrCh (if any)
				ticker.Stop()
				done <- struct{}{}
				c.events <- StateChange{turn, Quitting}
				close(c.events)
				return
			}

			world = w
			turn = p.Turns

			ticker.Stop()
			done <- struct{}{}

			aliveCells := AliveCells(world, p.ImageWidth, p.ImageHeight)
			c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
			saveImage(p, c, world, turn)
			c.events <- StateChange{turn, Quitting}
			close(c.events)
			return

		// Simulation error
		case err := <-simErrCh:
			fmt.Println("Simulation error from broker:", err)
			ticker.Stop()
			done <- struct{}{}
			c.events <- StateChange{turn, Quitting}
			close(c.events)
			return

		// Keypress handling (p, s, q, k)
		case key := <-keypress:
			// If keypress is nil (in tests), this case is disabled and never fires.
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
				// Save current known world
				saveImage(p, c, world, turn)

			case 'q':
				// Graceful quit — stop simulation but don't kill workers forever
				saveImage(p, c, world, turn)
				_ = client.Call("Broker.ControllerExit", Empty{}, &Empty{})
				ticker.Stop()
				done <- struct{}{}
				c.events <- StateChange{turn, Quitting}
				fmt.Println("Quitting...")
				close(c.events)
				return

			case 'k':
				// Hard kill all workers
				saveImage(p, c, world, turn)
				_ = client.Call("Broker.KillWorkers", Empty{}, &Empty{})
				ticker.Stop()
				done <- struct{}{}
				c.events <- StateChange{turn, Quitting}
				fmt.Println("Full system shutdown.")
				close(c.events)
				os.Exit(0)
			}

		default:
			// Avoid busy-spin when nothing is happening
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// AliveCells returns the list of live cells in the world.
func AliveCells(world [][]byte, width, height int) []util.Cell {
	cells := make([]util.Cell, 0)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == 255 {
				cells = append(cells, util.Cell{X: x, Y: y})
			}
		}
	}
	return cells
}

// saveImage writes the world to a PGM via the IO goroutine.
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

	c.events <- ImageOutputComplete{
		CompletedTurns: turn,
		Filename:       outFileName,
	}
}

// mergeSections stitches the worker sections (vertical slices) back into a full world.
func mergeSections(results [][][]byte) [][]byte {
	totalRows := 0
	for _, sec := range results {
		totalRows += len(sec)
	}
	if totalRows == 0 {
		return nil
	}

	width := len(results[0][0])
	world := make([][]byte, totalRows)
	row := 0
	for _, sec := range results {
		for _, line := range sec {
			world[row] = make([]byte, width)
			copy(world[row], line)
			row++
		}
	}
	return world
}
