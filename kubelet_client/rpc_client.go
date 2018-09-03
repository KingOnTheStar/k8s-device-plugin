package kubelet_client

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	api "k8s-device-plugin/kubelet_client/api"
	"log"
	"time"
)

const (
	address = "localhost:50052"
)

func SendTestString() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := api.NewGreeterClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	topoString := "GPU-102-"
	r, err := c.InformTopology(ctx, &api.TopologyRequest{
		Topo: topoString,
	})
	for err != nil {
		log.Printf("Fail, try again")
		time.Sleep(time.Second)
		r, err = c.InformTopology(ctx, &api.TopologyRequest{
			Topo: topoString,
		})
	}
	/*if err != nil {
		log.Fatalf("could not greet: %v", err)
	}*/
	log.Printf("Greeting: %s", r.Message)
}
