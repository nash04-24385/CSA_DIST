package gol

// SimulationArgs bundles Params + initial world so the broker
// can start the distributed simulation from the correct state.
type SimulationArgs struct {
	Params Params
	World  [][]byte
}
