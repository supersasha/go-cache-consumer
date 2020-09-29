package cache

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/gomodule/redigo/redis"
)

var ctx = context.Background()

type Config struct {
	URLs             []string `yaml:"URLs"`
	MinTimeout       int      `yaml:"MinTimeout"`
	MaxTimeout       int      `yaml:"MaxTimeout"`
	NumberOfRequests int      `yaml:"NumberOfRequests"`
	ErrorTimeout     int      `yaml:"ErrorTimeout"`
	RedisHostport    string   `yaml:"RedisHostport"`
}

func DefaultConfig() *Config {
	return &Config{
		NumberOfRequests: 1,
		MinTimeout:       300,
		MaxTimeout:       600,
		ErrorTimeout:     30,
		RedisHostport:    "127.0.0.1:6379",
	}
}

type Cache struct {
	config    *Config
	redisPool *redis.Pool
	subConn   redis.Conn
	subCh     chan dataReadySubscription
}

func NewCache(config *Config) *Cache {
	subCh := make(chan dataReadySubscription)
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", config.RedisHostport)
		},
		MaxActive: 100,
		Wait:      true,
	}
	subConn, err := redis.Dial("tcp", config.RedisHostport)
	if err != nil {
		log.Fatalf("establishing connection to Redis: %v", err)
	}
	go subscribeReady(subConn, subCh)
	return &Cache{
		config:    config,
		redisPool: redisPool,
		subConn:   subConn,
		subCh:     subCh,
	}
}

func (c *Cache) GetRandomDataStream(_ *Void, stream Cache_GetRandomDataStreamServer) error {
	ch := make(chan []byte)
	for i := 0; i < c.config.NumberOfRequests; i++ {
		url := c.config.URLs[rand.Intn(len(c.config.URLs))]
		go getData(url, c.redisPool, ch, c.subCh, c.config, 3)
	}
	for i := 0; i < c.config.NumberOfRequests; i++ {
		data := <-ch
		if err := stream.Send(&Data{Value: data}); err != nil {
			log.Printf("sending data: %v", err)
		}
	}
	return nil
}

func listenReadyMessages(c redis.Conn, messages chan<- string) {
	conn := redis.PubSubConn{Conn: c}
	defer conn.Close()

	conn.Subscribe("cache:ready")
	for {
		switch v := conn.Receive().(type) {
		case redis.Message:
			messages <- string(v.Data)
		default:
			// do nothing
		}
	}
}

func subscribeReady(conn redis.Conn, subscriptions <-chan dataReadySubscription) {
	messages := make(chan string)
	go listenReadyMessages(conn, messages)

	subscribers := make(map[string][]chan struct{})

	for {
		select {
		case sub := <-subscriptions:
			subscribers[sub.url] = append(subscribers[sub.url], sub.signal)
		case url := <-messages:
			ss := subscribers[url]
			for _, s := range ss {
				close(s)
			}
			subscribers[url] = nil
		}
	}
}

type cacheStatus int

const (
	ready cacheStatus = iota
	fetching
	fetch
	fetcherr
)

type cacheResult struct {
	data   []byte
	status cacheStatus
}

type dataReadySubscription struct {
	url    string
	signal chan struct{}
}

func getData(url string, pool *redis.Pool, out chan<- []byte, dataReadySubscribe chan<- dataReadySubscription, config *Config, attempts int) {
	if attempts == 0 {
		out <- nil
		return
	}
	dataReady := make(chan struct{}, 1)
	dataReadySubscribe <- dataReadySubscription{url: url, signal: dataReady}
	result, err := getDataFromCache(url, pool, config)
	if err != nil {
		getData(url, pool, out, dataReadySubscribe, config, attempts-1)
		return
	}
	switch result.status {
	case ready:
		out <- result.data
	case fetch:
		data := fetchUrl(url)
		if data != nil {
			out <- data
			go func() {
				putDataToCache(url, data, pool, config)
				publishReady(url, pool)
			}()
		} else {
			out <- data
			putErrorToCache(url, pool, config)
			publishReady(url, pool)
		}
	case fetching:
		<-dataReady
		getData(url, pool, out, dataReadySubscribe, config, attempts-1)
	case fetcherr:
		out <- nil
	}
}

func publishReady(url string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()
	conn.Do("PUBLISH", "cache:ready", url)
}

func putErrorToCache(url string, pool *redis.Pool, config *Config) {
	conn := pool.Get()
	defer conn.Close()
	key := "cache:" + url
	conn.Do("HSET", key, "data", "")
	conn.Do("HSET", key, "status", "fetcherr")
	conn.Do("EXPIRE", key, config.ErrorTimeout)
}

func putDataToCache(url string, data []byte, pool *redis.Pool, config *Config) {
	conn := pool.Get()
	defer conn.Close()
	key := "cache:" + url
	conn.Do("HSET", key, "data", data)
	conn.Do("HSET", key, "status", "ready")
	ttl := rand.Intn(config.MaxTimeout-config.MinTimeout) + config.MinTimeout
	conn.Do("EXPIRE", key, ttl)
}

func getDataFromCache(url string, pool *redis.Pool, config *Config) (result *cacheResult, err error) {
	conn := pool.Get()
	defer conn.Close()

	key := "cache:" + url
	_, err = conn.Do("WATCH", key)
	if err != nil {
		return nil, err
	}

	defer conn.Do("UNWATCH")
	res, err := redis.StringMap(conn.Do("HGETALL", key))
	if err != nil {
		return nil, err
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
	case "fetcherr":
		result = &cacheResult{
			status: fetcherr,
		}
	default:
		if err := conn.Send("MULTI"); err != nil {
			return nil, err
		}
		if err := conn.Send("HSET", key, "status", "fetching"); err != nil {
			return nil, err
		}
		if err := conn.Send("EXPIRE", key, config.ErrorTimeout+1); err != nil {
			return nil, err
		}
		q, err := conn.Do("EXEC")
		if err != nil {
			return nil, err
		}
		if q == nil {
			return nil, fmt.Errorf("transaction failed")
		}
		result = &cacheResult{
			status: fetch,
		}
	}

	return result, nil
}

func fetchUrl(url string) []byte {
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		log.Printf("Error fetching %q: %v", url, err)
		return nil
	}
	data, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Printf("Error reading body for %q: %v", url, err)
		return nil
	}
	return data
}
