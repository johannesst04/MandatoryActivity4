package node

import (
	"MandatoryActivity4/proto"
	"context"
	"sync"
	"fmt"
	"time"
)



type Node struct {
    proto.UnimplementedMutexServer

    id int32

    requesting bool
    inCriticalSection bool

    requestTimestamp int64

    receivedReplies map[int32]bool
    deferredReplies map[int32]bool

    peers map[int32]proto.MutexClient

    mu sync.Mutex
}

func NewNode(id int32) *Node {
    return &Node{
        id: id,
        receivedReplies: make(map[int32]bool),
        deferredReplies: make(map[int32]bool),
        peers: make(map[int32]proto.MutexClient),
    }
}

func (n *Node) Request(ctx context.Context, req *proto.CallRequest) (*proto.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.requesting && !n.inCriticalSection{
		fmt.Printf("NODE %d: Permission granted to node: %d\n",n.id,req.GetId())
		if err := n.grantPermission(req.GetId()); err != nil {
        return nil, err
    	}
		}else if n.inCriticalSection{
		n.deferredReplies[req.GetId()] = true
		fmt.Printf("NODE %d: Deferred request from node: %d\n",n.id,req.GetId())
	}else if n.requesting{
		if req.GetTimestamp() < n.requestTimestamp{
			fmt.Printf("NODE %d: Permission granted to node: %d\n",n.id,req.GetId())
			if err := n.grantPermission(req.GetId()); err != nil {
        	return nil, err
    		}
		}else if req.GetTimestamp() == n.requestTimestamp{
			if req.GetId() < n.id{
				fmt.Printf("NODE %d: Permission granted to node: %d\n",n.id,req.GetId())
				if err := n.grantPermission(req.GetId()); err != nil {
        		return nil, err
    			}
			}else{
				n.deferredReplies[req.GetId()] = true
				fmt.Printf("NODE %d: Deferred request from node: %d\n",n.id,req.GetId())
			}
		}else{
			n.deferredReplies[req.GetId()] = true
			fmt.Printf("NODE %d: Deferred request from node: %d\n",n.id,req.GetId())
		}
	}

	return &proto.Empty{}, nil
}

func (n *Node) Reply(ctx context.Context, reply *proto.MsgReply) (*proto.Empty, error) {

    n.mu.Lock()
    defer n.mu.Unlock()

    n.receivedReplies[reply.GetId()] = true

    fmt.Printf(
        "Node %d: Received reply from node %d\n",
        n.id,
        reply.GetId(),
    )

    if n.requesting && len(n.receivedReplies) == len(n.peers) {
        go n.EnterCriticalSection()
    }

    return &proto.Empty{}, nil
}

func (n *Node) EnterCriticalSection() {
    n.mu.Lock()

    n.inCriticalSection = true

    n.mu.Unlock()

    fmt.Printf("NODE %d: Entered critical section\n", n.id)

    // Simulated critical section
    time.Sleep(2 * time.Second)

    fmt.Printf("NODE %d: Exited critical section\n", n.id)

    n.mu.Lock()

    n.inCriticalSection = false
    n.requesting = false

    deferred := make([]int32, 0)

    for id := range n.deferredReplies {
        deferred = append(deferred, id)
    }

    n.deferredReplies = make(map[int32]bool)

    n.mu.Unlock()

    for _, id := range deferred {
        if err := n.grantPermission(id); err != nil {
            fmt.Printf(
                "Failed to grant deferred permission to %d: %v\n",
                id,
                err,
            )
        }
    }
}

func (n *Node) RequestCriticalSection() {
    n.mu.Lock()

    n.requesting = true
    n.requestTimestamp = time.Now().UnixNano()

    // clear replies from previous round
    n.receivedReplies = make(map[int32]bool)

    timestamp := n.requestTimestamp

    n.mu.Unlock()

    fmt.Printf(
        "NODE %d: Requesting critical section at timestamp %d\n",
        n.id,
        timestamp,
    )

    for _, peer := range n.peers {
        _, err := peer.Request(
            context.Background(),
            &proto.CallRequest{
                Timestamp: timestamp,
                Id:        n.id,
            },
        )

        if err != nil {
            fmt.Printf("Request failed: %v\n", err)
        }
    }
}

func (n *Node) grantPermission(nodeID int32) error {
    client := n.peers[nodeID]

    _, err := client.Reply(
        context.Background(),
        &proto.MsgReply{
            Id: n.id,
        },
    )

    return err
}

func (n *Node) AddPeer(
    id int32,
    client proto.MutexClient,
) {
    n.mu.Lock()
    defer n.mu.Unlock()

    n.peers[id] = client
}

	
