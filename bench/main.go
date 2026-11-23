package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	_ "go.uber.org/automaxprocs"

	pb "bench/datastore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr     = flag.String("addr", "localhost:7777", "server address")
	conns    = flag.Int("conns", 1, "number of gRPC connections")
	workers  = flag.Int("c", 20, "concurrent requests in flight, spread over all connections")
	total    = flag.Int("n", 400000, "total requests")
	getPct   = flag.Int("get", 0, "percent of requests that are Get")
	queryPct = flag.Int("query", 0, "percent of requests that are Query (wildcard over one branch)")
	keyspace = flag.Int("keys", 400000, "number of distinct keys")
	prefill  = flag.Bool("prefill", false, "set every key once before the timed run")
	ttl      = flag.Int64("ttl", 20, "ttl in seconds for Set (0 = none)")
	batch    = flag.Int("batch", 0, "items per SetBatch; 0 sends unary Set. Get and Query are not batched")
	history  = flag.Bool("history", false, "set preserve_history on writes")
	subs     = flag.Int("subscribers", 0, "subscribers to run during the timed run")
	pattern  = flag.String("pattern", ">", "subscription pattern")
)

const branches = 1000

func key(r int) string { return fmt.Sprintf("ds.yo.%d.k%d", r%branches, r) }

func main() {
	flag.Parse()

	clients := make([]pb.DatastoreClient, *conns)
	for i := range clients {
		conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("connect: %v", err)
		}
		defer conn.Close()
		clients[i] = pb.NewDatastoreClient(conn)
	}
	ctx := context.Background()
	value := []byte("some value")

	if *prefill {
		run(*keyspace, clients, func(c pb.DatastoreClient, i int, _ *rand.Rand) error {
			_, err := c.Set(ctx, setRequest(key(i), value, 0))
			return err
		})
	}

	var events atomic.Int64
	for i := 0; i < *subs; i++ {
		go subscribe(clients[i%len(clients)], &events)
	}
	if *subs > 0 {
		defer func() { fmt.Printf("subscriber events received=%d\n", events.Load()) }()
	}

	if *batch > 0 {
		runBatches(clients, value)
		return
	}

	start := time.Now()
	lat, errs := run(*total, clients, func(c pb.DatastoreClient, _ int, rng *rand.Rand) error {
		r := rng.IntN(*keyspace)
		p := rng.IntN(100)
		var err error
		switch {
		case p < *getPct:
			_, err = c.Get(ctx, &pb.GetRequest{Key: key(r)})
		case p < *getPct+*queryPct:
			_, err = c.Query(ctx, &pb.QueryRequest{Key: fmt.Sprintf("ds.yo.%d.*", r%branches)})
		default:
			_, err = c.Set(ctx, setRequest(key(r), value, 0))
		}
		return err
	})
	elapsed := time.Since(start)

	fmt.Printf("conns=%d c=%d n=%d get=%d%% query=%d%%\n", *conns, *workers, *total, *getPct, *queryPct)
	report(*total, elapsed, lat, errs)
}

func setRequest(key string, value []byte, ts int64) *pb.SetRequest {
	return &pb.SetRequest{
		Key:         key,
		Value:       value,
		TimestampNs: ts,
		Options:     &pb.SetOptions{Ttl: *ttl, PreserveHistory: *history},
	}
}

// runBatches sends *total items as SetBatch calls like a collector: each batch carries
// source timestamps and is retried unchanged until acked. Latency is per batch.
func runBatches(clients []pb.DatastoreClient, value []byte) {
	ctx := context.Background()
	var retries, rejected atomic.Int64
	batches := *total / *batch
	start := time.Now()
	lat, _ := run(batches, clients, func(c pb.DatastoreClient, _ int, rng *rand.Rand) error {
		ts := time.Now().UnixNano()
		req := &pb.SetBatchRequest{Items: make([]*pb.SetRequest, *batch)}
		for j := range req.Items {
			req.Items[j] = setRequest(key(rng.IntN(*keyspace)), value, ts+int64(j))
		}
		for {
			resp, err := c.SetBatch(ctx, req)
			if err == nil {
				rejected.Add(int64(len(resp.Rejected)))
				return nil
			}
			retries.Add(1)
		}
	})
	elapsed := time.Since(start)

	fmt.Printf("conns=%d c=%d items=%d batch=%d\n", *conns, *workers, batches**batch, *batch)
	report(batches**batch, elapsed, lat, retries.Load())
	fmt.Printf("rejected=%d\n", rejected.Load())
}

// report prints throughput in operations (items, for batches) and latency per request.
func report(ops int, elapsed time.Duration, lat []time.Duration, errs int64) {
	slices.Sort(lat)
	pct := func(q float64) time.Duration { return lat[int(q*float64(len(lat)-1))] }
	fmt.Printf("elapsed=%s rps=%.0f errors=%d\n", elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds(), errs)
	fmt.Printf("p50=%s p99=%s p999=%s max=%s\n", pct(0.5), pct(0.99), pct(0.999), lat[len(lat)-1])
}

// run executes n ops over *workers goroutines and returns per-op latencies and the error count.
func run(n int, clients []pb.DatastoreClient, op func(pb.DatastoreClient, int, *rand.Rand) error) ([]time.Duration, int64) {
	var next, errs atomic.Int64
	lat := make([]time.Duration, n)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(c pb.DatastoreClient, rng *rand.Rand) {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				t := time.Now()
				if err := op(c, i, rng); err != nil {
					errs.Add(1)
				}
				lat[i] = time.Since(t)
			}
		}(clients[w%len(clients)], rand.New(rand.NewPCG(uint64(w), 1)))
	}
	wg.Wait()
	return lat, errs.Load()
}

// subscribe counts every event it receives until the stream ends.
func subscribe(c pb.DatastoreClient, events *atomic.Int64) {
	stream, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{Pattern: *pattern})
	if err != nil {
		log.Printf("subscribe: %v", err)
		return
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("subscription ended: %v", err)
			return
		}
		events.Add(int64(len(msg.Events)))
	}
}
