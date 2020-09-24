package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"

	"google.golang.org/grpc"

	"cache"
)

func call(wg *sync.WaitGroup, hostport string) {

	defer wg.Done()
	conn, err := grpc.Dial(hostport, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	client := cache.NewCacheClient(conn)

	void := &cache.Void{}
	stream, err := client.GetRandomDataStream(context.Background(), void)
	if err != nil {
		log.Fatalf("client error: %v", err)
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("receiving data: %v", err)
		}
		fmt.Print(".")
	}
}

func main() {
	hostport := flag.String("s", "127.0.0.1:8511", "cache service host:port")
	flag.Parse()

	n := 1000
	wg := &sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go call(wg, *hostport)
	}
	wg.Wait()
	fmt.Println()
}
