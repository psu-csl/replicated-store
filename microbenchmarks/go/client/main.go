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
	"sync/atomic"
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

	var numResponse int64 = 0
	sum := 0

	go func() {
		var prevNumResponse int64 = 0
		for {
			time.Sleep(1 * time.Second)
			currentNumResponse := numResponse
			log.Printf("throughput %v op/s\n", currentNumResponse-prevNumResponse)
			prevNumResponse = currentNumResponse
		}
	}()

	// Contact the server and print out its response.
	ctx := context.Background()
	//defer cancel()
	for i := 0; i < *numRequests; i++ {
		reply, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		atomic.AddInt64(&numResponse, 1)
		sum += len(reply.String())
	}
	log.Printf("sum: %v\n", sum)
}
