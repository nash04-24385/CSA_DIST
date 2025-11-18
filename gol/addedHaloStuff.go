package gol

type WorkerInitRequest struct {
	Params     Params
	StartY     int      // global start row this worker owns
	EndY       int      // global end row (exclusive)
	LocalWorld [][]byte // initial slice
	AboveAddr  string   // "ip:8030" of worker above
	BelowAddr  string   // "ip:8030" of worker below

}

type HaloRow struct {
	Row []byte
}
