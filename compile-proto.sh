#!/bin/bash

protoc cache.proto --go_out=plugins=grpc:src/cache
