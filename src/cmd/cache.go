package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"cache"
)

func main() {
	configPath := flag.String("c", "./config.yml", "path to config")
	port := flag.String("p", "8511", "listen port")
	flag.Parse()

	config := cache.DefaultConfig()
	data, err := ioutil.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("reading config: %v", err)
	}

	err = yaml.Unmarshal(data, config)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	cfg, _ := yaml.Marshal(config)
	fmt.Println(string(cfg))

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	cache.RegisterCacheServer(grpcServer, cache.NewCache(config))
	grpcServer.Serve(lis)
}
