// halo_local_test.go
package main

import (
	"testing"

	"uk.ac.bris.cs/gameoflife/gol"
)

// ---- helpers ----

func cloneRows(world [][]byte, start, end int) [][]byte {
	h := end - start
	w := len(world[0])
	out := make([][]byte, h)
	for i := 0; i < h; i++ {
		out[i] = make([]byte, w)
		copy(out[i], world[start+i])
	}
	return out
}

func cloneRow(row []byte) []byte {
	r := make([]byte, len(row))
	copy(r, row)
	return r
}

func makeWorld(h, w int) [][]byte {
	m := make([][]byte, h)
	for y := 0; y < h; y++ {
		m[y] = make([]byte, w)
	}
	return m
}

func worldsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for y := range a {
		if len(a[y]) != len(b[y]) {
			return false
		}
		for x := range a[y] {
			if a[y][x] != b[y][x] {
				return false
			}
		}
	}
	return true
}

// One full sequential GoL step on the *whole* world.
// This is our "golden" reference – it doesn't use halos at all.
func sequentialStep(p gol.Params, world [][]byte) [][]byte {
	h := p.ImageHeight
	w := p.ImageWidth
	alive := byte(255)

	next := make([][]byte, h)
	for y := 0; y < h; y++ {
		next[y] = make([]byte, w)
		for x := 0; x < w; x++ {
			// wrap-around indices
			left := (x - 1 + w) % w
			right := (x + 1) % w
			up := (y - 1 + h) % h
			down := (y + 1) % h

			count := 0
			if world[y][left] == alive {
				count++
			}
			if world[y][right] == alive {
				count++
			}
			if world[up][x] == alive {
				count++
			}
			if world[down][x] == alive {
				count++
			}
			if world[up][left] == alive {
				count++
			}
			if world[up][right] == alive {
				count++
			}
			if world[down][left] == alive {
				count++
			}
			if world[down][right] == alive {
				count++
			}

			if world[y][x] == alive {
				if count == 2 || count == 3 {
					next[y][x] = alive
				} else {
					next[y][x] = 0
				}
			} else {
				if count == 3 {
					next[y][x] = alive
				} else {
					next[y][x] = 0
				}
			}
		}
	}
	return next
}

// build a 16x16 world with a pattern that crosses the worker boundary
// (rows 7 and 8) so halos actually matter.
func makeInitialTestWorld16x16() [][]byte {
	h, w := 16, 16
	world := makeWorld(h, w)

	alive := byte(255)

	// Simple vertical bar that crosses the boundary between row 7 and 8:
	// positions (5,6), (5,7), (5,8), (5,9)
	world[6][5] = alive
	world[7][5] = alive
	world[8][5] = alive
	world[9][5] = alive

	// And a little horizontal bit just to mix things up:
	// (4,7), (5,7), (6,7)
	world[7][4] = alive
	world[7][6] = alive

	return world
}

// ---- the actual test ----

func TestLocalHaloMatchesSequential_16x16_2workers(t *testing.T) {
	p := gol.Params{
		ImageWidth:  16,
		ImageHeight: 16,
		Turns:       1,
		Threads:     1,
	}

	world := makeInitialTestWorld16x16()

	const turns = 50
	for turn := 0; turn < turns; turn++ {
		// 1. Golden full-world step
		golden := sequentialStep(p, world)

		// 2. Split world into two workers [0,8) and [8,16)
		w0Local := cloneRows(world, 0, 8)
		w1Local := cloneRows(world, 8, 16)

		// 3. Build correct halos from the *global* world (with vertical wrap)
		top0 := cloneRow(world[15])   // row above 0
		bottom0 := cloneRow(world[8]) // row below 7

		top1 := cloneRow(world[7])    // row above 8
		bottom1 := cloneRow(world[0]) // row below 15 (wrap)

		// 4. Use your halo update for each worker
		// (calculateNextUsingHalos is defined in worker.go in package main)
		w0Next := calculateNextUsingHalos(p, w0Local, top0, bottom0)
		w1Next := calculateNextUsingHalos(p, w1Local, top1, bottom1)

		// 5. Stitch back together
		stitched := makeWorld(p.ImageHeight, p.ImageWidth)
		copy(stitched[0:8], w0Next)
		copy(stitched[8:16], w1Next)

		// 6. Compare halo-based distributed vs golden sequential
		if !worldsEqual(stitched, golden) {
			// find first mismatch to help debug
			for y := 0; y < p.ImageHeight; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if stitched[y][x] != golden[y][x] {
						t.Fatalf("mismatch at turn %d cell (%d,%d): halo=%d seq=%d",
							turn+1, x, y, stitched[y][x], golden[y][x])
					}
				}
			}
		}

		// 7. advance for next turn
		world = golden
	}
}
