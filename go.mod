module main

go 1.13

require google.golang.org/grpc v1.32.0

require (
	cache v1.0.0
	github.com/go-redis/redis/v8 v8.2.0 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)

replace cache => ./src/cache
