package main

import (
    "log"
    "net"
    "time"
    "MandatoryActivity4/node"
    "MandatoryActivity4/proto"
    "google.golang.org/grpc"
)

func startServer(n *node.Node, port string) {
    lis, err := net.Listen("tcp", port)
    if err != nil {
        log.Fatal(err)
    }

    grpcServer := grpc.NewServer()

    proto.RegisterMutexServer(grpcServer, n)

    go func() {
        if err := grpcServer.Serve(lis); err != nil {
            log.Fatal(err)
        }
    }()
}

func main() {

    node1 := node.NewNode(1)
    node2 := node.NewNode(2)
    node3 := node.NewNode(3)

    startServer(node1, ":5001")
    startServer(node2, ":5002")
    startServer(node3, ":5003")

    time.Sleep(time.Second)

    // TODO:
    // Create grpc client connections
	conn12, _ := grpc.Dial("localhost:5002", grpc.WithInsecure())
	conn13, _ := grpc.Dial("localhost:5003", grpc.WithInsecure())

	conn21, _ := grpc.Dial("localhost:5001", grpc.WithInsecure())
	conn23, _ := grpc.Dial("localhost:5003", grpc.WithInsecure())

	conn31, _ := grpc.Dial("localhost:5001", grpc.WithInsecure())
	conn32, _ := grpc.Dial("localhost:5002", grpc.WithInsecure())
	
    // Create MutexClient objects

    // Add peers to node1, node2, node3
	node1.AddPeer(2, proto.NewMutexClient(conn12))
	node1.AddPeer(3, proto.NewMutexClient(conn13))

	node2.AddPeer(1, proto.NewMutexClient(conn21))
	node2.AddPeer(3, proto.NewMutexClient(conn23))

	node3.AddPeer(1, proto.NewMutexClient(conn31))
	node3.AddPeer(2, proto.NewMutexClient(conn32))

    // Trigger RequestCriticalSection()
	go node1.RequestCriticalSection()

	time.Sleep(time.Second)

	go node2.RequestCriticalSection()

    select {}
}