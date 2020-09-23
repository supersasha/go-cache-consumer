package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"cache"
)

func main() {
	config := &cache.Config{}
	data, _ := ioutil.ReadFile("./config.yml")
	fmt.Printf("data: %s", data)

	err := yaml.Unmarshal(data, config)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}
	fmt.Println("config:", config)

	lis, err := net.Listen("tcp", ":8511")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	cache.RegisterCacheServer(grpcServer, cache.NewCache(config))
	grpcServer.Serve(lis)
}
