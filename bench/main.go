package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "go.uber.org/automaxprocs"

	pb "bench/datastore"
	"google.golang.org/grpc"
)

func randomKey(prefix string, length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return prefix + string(b)
}

func sendRequests(client pb.DatastoreClient, wg *sync.WaitGroup, reqNum int, countCh chan<- int) {
	defer wg.Done()
	var count int
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	loops := 20000

	for i := 0; i < loops; i++ {
		key := randomKey(fmt.Sprintf("ds.yo.%d.", i), 8)
		_, err := client.Set(ctx, &pb.SetRequest{
			Key:   key,
			Value: []byte("some value"),
			Options: &pb.SetOptions{
				Ttl: 200,
			},
		})
		if err != nil {
			log.Printf("Error setting key %s: %v", key, err)
			continue
		}
		count++
	}
	countCh <- count
}

func main() {
	start := time.Now()
	conn, err := grpc.Dial("localhost:7777", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewDatastoreClient(conn)

	var wg sync.WaitGroup
	countCh := make(chan int, 200)
	totalCount := 0

	num_threads := 20

	for i := 0; i < num_threads; i++ {
		wg.Add(1)
		go sendRequests(client, &wg, i, countCh)
	}

	go func() {
		wg.Wait()
		close(countCh)
	}()

	for count := range countCh {
		totalCount += count
	}

	elapsed := time.Since(start)
	fmt.Printf("Total keys set: %d\n", totalCount)
	fmt.Printf("Elapsed time: %s\n", elapsed)
}
