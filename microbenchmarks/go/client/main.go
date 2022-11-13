/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

const (
	defaultName = "world"
)

var (
	addr        = flag.String("addr", "localhost:10000", "the address to connect to")
	name        = flag.String("name", defaultName, "Name to greet")
	numRequests = flag.Int("n", 100000, "number of requests")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	responseChan := make(chan int64, 1000000)

	go output(responseChan)

	// Contact the server and print out its response.
	for i := 0; i < *numRequests; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		responseChan <- 1
	}
}

func output(responseChan chan int64) {
	beginTime := time.Now()
	for {
		time.Sleep(1 * time.Second)

		numResponse := len(responseChan)
		endTime := time.Now()
		throughput := float64(numResponse) / endTime.Sub(beginTime).Seconds()
		log.Printf("throughput %v op/s\n", throughput)
	}
}
