package cache

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Config struct {
	URLs             []string `yaml:"URLs"`
	MinTimeout       int      `yaml:"MinTimeout"`
	MaxTimeout       int      `yaml:"MaxTimeout"`
	NumberOfRequests int      `yaml:"NumberOfRequests"`
}

type Cache struct {
	config *Config
	rdbs   []*redis.Client
	rdbSub *redis.Client
	subCh  chan dataReadySubscription
}

func NewCache(config *Config) *Cache {
	fmt.Println("Config: ", config)
	rdbs := make([]*redis.Client, config.NumberOfRequests)
	for i := 0; i < config.NumberOfRequests; i++ {
		rdbs[i] = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
		})
	}
	rdbSub := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	subCh := make(chan dataReadySubscription)
	go subscribeReady(rdbSub, subCh)
	return &Cache{
		config: config,
		rdbs:   rdbs,
		rdbSub: rdbSub,
		subCh:  subCh,
	}
}

func subscribeReady(rdb *redis.Client, subscriptions <-chan dataReadySubscription) {
	pubsub := rdb.Subscribe(ctx, "cache:ready")
	defer pubsub.Close()
	pubsub.Receive(ctx)
	ch := pubsub.Channel()

	subscribers := make(map[string][]chan struct{})

	for {
		select {
		case sub := <-subscriptions:
			subscribers[sub.url] = append(subscribers[sub.url], sub.signal)
		case msg := <-ch:
			url := msg.Payload
			ss := subscribers[url]
			for _, s := range ss {
				close(s)
			}
			subscribers[url] = nil
		}
	}
}

func (c *Cache) GetRandomDataStream(_ *Void, stream Cache_GetRandomDataStreamServer) error {
	ch := make(chan []byte)
	for i := 0; i < c.config.NumberOfRequests; i++ {
		url := c.config.URLs[rand.Intn(len(c.config.URLs))]
		go getData(url, c.rdbs[i], ch, c.subCh, c.config)
	}
	for i := 0; i < c.config.NumberOfRequests; i++ {
		data := <-ch
		if data == nil {
			continue
		}
		if err := stream.Send(&Data{Value: data}); err != nil {
			log.Printf("sending data: %v", err)
		}
	}
	return nil
}

type cacheStatus int

const (
	ready cacheStatus = iota
	fetching
	fetch
)

type cacheResult struct {
	data   []byte
	status cacheStatus
}

type dataReadySubscription struct {
	url    string
	signal chan struct{}
}

func getData(url string, rdb *redis.Client, out chan<- []byte, dataReadySubscribe chan<- dataReadySubscription, config *Config) {
	dataReady := make(chan struct{}, 1)
	dataReadySubscribe <- dataReadySubscription{url: url, signal: dataReady}
	result, err := getDataFromCache(url, rdb)
	if err != nil {
		getData(url, rdb, out, dataReadySubscribe, config)
	}
	switch result.status {
	case ready:
		out <- result.data
	case fetch:
		data := fetchUrl(url)
		out <- data
		go func() {
			putDataToCache(url, data, rdb, config)
			publishReady(url, rdb)
		}()
	case fetching:
		fmt.Println("Waiting someone fetching", url)
		<-dataReady
		getData(url, rdb, out, dataReadySubscribe, config)
	}
}

func publishReady(url string, rdb *redis.Client) {
	rdb.Publish(ctx, "cache:ready", url)
}

func putDataToCache(url string, data []byte, rdb *redis.Client, config *Config) {
	key := "cache:" + url
	rdb.HSet(ctx, key, "data", data)
	rdb.HSet(ctx, key, "status", "ready")
	seconds := rand.Intn(config.MaxTimeout-config.MinTimeout) + config.MinTimeout
	ttl := time.Duration(seconds) * time.Second
	rdb.Expire(ctx, key, ttl)
}

func getDataFromCache(url string, rdb *redis.Client) (result *cacheResult, err error) {
	key := "cache:" + url
	err = rdb.Watch(ctx, func(tx *redis.Tx) error {
		res, err := tx.HGetAll(ctx, key).Result()
		if err != nil {
			return err
		}
		switch res["status"] {
		case "ready":
			result = &cacheResult{
				data:   []byte(res["data"]),
				status: ready,
			}
		case "fetching":
			result = &cacheResult{
				status: fetching,
			}
		default:
			tx.HSet(ctx, key, "status", "fetching")
			result = &cacheResult{
				status: fetch,
			}
		}
		return nil
	}, key)
	return
}

func fetchUrl(url string) []byte {
	log.Printf("fetching %q...", url)
	res, err := http.Get(url)
	if err != nil {
		return nil
	}
	data, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil
	}
	log.Printf("fetched %q.", url)
	return data
}
