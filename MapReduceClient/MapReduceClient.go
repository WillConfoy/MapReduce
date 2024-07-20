package main


import (
	"fmt"
	"os"
	// "errors"
	"log"
	// "math/rand"
	// "net"
	// "strings"
	"time"
	// "bufio"
	// "strconv"
	"flag"
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "cs498.com/MapReduce/MapReduce"
	user "cs498.com/MapReduce/usermapreduce"
)



var (
	addr = flag.String("addr", "localhost:50052", "the address to connect to")
	mapCh = make(chan [][]string)
	redCh = make(chan []string)
	// name = flag.String("name", defaultName, "Name to greet")
	// key = flag.String("key", defaultKey, "Key to get")
)

func check(e error) {
    if e != nil {
		fmt.Println("PANIC PANIC PANIC")
		log.Fatalf("Got error %v", e)
    }
}

// Loop: ask for a task -> do the task -> write output to a file
// instead of writing out to a file, make an RPC call that just puts data into a data structure on the server
// basically just Put()

func HandleTasks(ctx context.Context, c pb.MapReduceServiceClient) {
	for {
		r, err := c.AskForTask(ctx, &pb.TaskRequest{Ip: "127.0.0.1"})
		check(err)
		log.Printf("%v task given, key: %v, val: %v", r.GetType(), r.GetKey(), r.GetValue())
		// ask for a task
		// if r.gettask is map or reduce or whatever
		if r.GetType() == "map" {
			ret := user.Map(r.GetKey(), r.GetValue())
			// xs := []*pb.MapResult{}
			// for _, x := range ret {
			// 	xs = append(xs, &pb.MapResult{Key: x[0], Value: x[1]})
			// }
			// mapCh <- pb.MapResultMessage{Results: xs}
			mapCh <- ret

		} else if r.GetType() == "reduce" {
			ret := user.Reduce(r.GetKey(), r.GetAllvals())
			redCh <- ret

		} else if r.GetType() == "die" {
			log.Printf("Got told to die")
			os.Exit(0)
		}
	}
}

func sendMapResults(ctx context.Context, c pb.MapReduceServiceClient) {
	for {
		send := <- mapCh
		xs := []*pb.MapResult{}
		for _, x := range send {
			xs = append(xs, &pb.MapResult{Key: x[0], Value: x[1]})
		}
		_, err := c.SendMapResult(ctx, &pb.MapResultMessage{Results: xs})
		check(err)
	}
}

func sendReduceResults(ctx context.Context, c pb.MapReduceServiceClient) {
	for {
		send := <- redCh
		_, err := c.SendReduceResult(ctx, &pb.ReduceResultMessage{Key: send[0], Value: send[1]})
		check(err)
	}
}


func main() {
	// set up connection
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMapReduceServiceClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
	defer cancel()

	go sendMapResults(ctx, c)
	go sendReduceResults(ctx, c)

	HandleTasks(ctx, c)
}