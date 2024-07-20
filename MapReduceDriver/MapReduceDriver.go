package main

import (
	"fmt"
	"os"
	// "errors"
	// "math/rand"
	// "net"
	"strings"
	// "time"
	"bufio"
	// "strconv"
	"sync"
	"context"
	"flag"
	"log"
	"net"
	"google.golang.org/grpc"
	pb "cs498.com/MapReduce/MapReduce"
)

type server struct {
	pb.UnimplementedMapReduceServiceServer
}

type redJob struct {
	index string
	element []string
}
// go doUserReduce(conns[it % len(conns)], index, element, newInChan)

var (
	port = flag.Int("port", 50052, "The server port")
	mapCh = make(chan [][]string)
	redCh = make(chan []string)
	mapConn = make(map[string][]string)
	redConn = make(map[string][]*redJob)
	mapDone = false
	allDone = false
	filename string
	m sync.Mutex

	// old ones
	// inCh = make(chan [][]string)
	outCh = make(chan map[string][]string)
	// newInChan = make(chan []string)
	newOutChan = make(chan map[string]string)
)


func sendData(conns []string, data []string, inCh chan [][]string) map[string]string {
	// var dataMap map[net.Conn]string
	filename = data[0]
	dataMap := make(map[string]string)
	stringSplit := strings.Split(data[1], "\n")
	for index, element := range stringSplit {
		dataMap[element] = stringSplit[index]
		mapConn[conns[index % len(conns)]] = append(mapConn[conns[index % len(conns)]], element)
		// tempSlice := []string{data[0], element}
		// go analyze(conns[index % len(conns)], tempSlice, inCh)
	}
	return dataMap
}

func doReduction(conns []string, data map[string][]string, newInChan chan []string) {
	it := 0
	for index, element := range data {
		// redConn[conns[index % len(conns)]] = append(redConn[conns[index % len(conns)]], element)
		newRedJob := redJob{index: index, element: element}
		redConn[conns[it % len(conns)]] = append(redConn[conns[it%len(conns)]], &newRedJob)
		// go doUserReduce(conns[it % len(conns)], index, element, newInChan)
		it++
	}
}

func (s *server) AskForTask (ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	if !mapDone {
		toDo := ""
		if len(mapConn[in.GetIp()]) > 1 {
			m.Lock()
			toDo, mapConn[in.GetIp()] = mapConn[in.GetIp()][0], mapConn[in.GetIp()][1:]
			m.Unlock()
		} else {
			toDo = mapConn[in.GetIp()][0]
			// mapConn[in.GetIp()] = []string{}
		}
		return &pb.TaskResponse{Type: "map", Key: filename, Value: toDo}, nil

	} else if !allDone {
		toDo := &redJob{}
		if len(redConn[in.GetIp()]) > 1 {
			m.Lock()
			toDo, redConn[in.GetIp()] = redConn[in.GetIp()][0], redConn[in.GetIp()][1:]
			m.Unlock()
		} else {
			toDo = redConn[in.GetIp()][0]
			// redConn[in.GetIp()] = []*redJob{}
		}
		return &pb.TaskResponse{Type: "reduce", Key: toDo.index, Allvals: toDo.element}, nil
	}
	return &pb.TaskResponse{Type: "die"}, nil
}

// func doUserReduce(conn string, key string, vals []string, newInChan chan []string) {

// 	results := reduceFunc(key, vals)

// 	newInChan <- results
// }

// func analyze(conn string, data []string, inCh chan [][]string) {
// 	results := mapFunc(data[0], data[1])

// 	// fmt.Println(results)

// 	inCh <- results
// }


func aggregate(inCh chan [][]string, outCh chan map[string][]string, num int) {
	bigMap := make(map[string][]string)
	for i :=0; i < num; i++{
		newList := <- inCh

		for _, k := range newList {
			bigMap[k[0]] = append(bigMap[k[0]], k[1])
		}

	}

	outCh <- bigMap
}

func redAggregate(redCh chan []string, newOutChan chan map[string]string, num int) {
	bigMap := make(map[string]string)
	for i :=0; i < num; i++{
		newList := <- redCh
		
		bigMap[newList[0]] = newList[1]

	}

	newOutChan <- bigMap
}

func check(e error) {
    if e != nil {
		fmt.Println("PANIC PANIC PANIC")
        log.Fatalf("Fatal error: %v", e)
    }
}

func readFile(path string, lengthCh chan int) string {
	file, err := os.Open(path)
	check(err)
	scanner := bufio.NewScanner(file)
	ret := ""
	numLines := 1
	for scanner.Scan() {
		ret += scanner.Text()
		ret += "\n"
		numLines++
	}
	go sendInt(numLines, lengthCh)
	return ret
}

func sendInt(num int, lengthCh chan int) {
	lengthCh <- num
}


func (s *server) SendMapResult(ctx context.Context, in *pb.MapResultMessage) (*pb.Bool, error) {
	// turn the map result message into [][]string
	vals := in.GetResults()
	temp := make([][]string, len(vals))
	for i := range temp {
		temp[i] = make([]string, 2)
	}

	for i, x := range vals {
		temp[i][0] = x.Key
		temp[i][1] = x.Value
	}

	mapCh <- temp
	return &pb.Bool{Success: true}, nil
}

func (s *server) SendReduceResult(ctx context.Context, in *pb.ReduceResultMessage) (*pb.Bool, error) {
	// turn the map result message into [][]string
	redCh <- []string{in.GetKey(), in.GetValue()}
	return &pb.Bool{Success: true}, nil
}


func main() {
	// FORMAT OF ARGS: [IP LIST, DATA SOURCE]
	// inCh := make(chan [][]string)
	// outCh := make(chan map[string][]string)
	lengthCh := make(chan int)


	// newMessages := os.Args[3]
	f, err := os.Open("./args.txt")
	scanner := bufio.NewScanner(f)

    check(err)
	scanner.Scan()
	args := strings.Split(scanner.Text(), " ")
    // fmt.Println(strings.Split(args[0], ","))
	strData := readFile(args[1], lengthCh)
	kvpair := []string{args[1], strData}
	conns := strings.Split(args[0], ",")
	i := <- lengthCh
	go aggregate(mapCh, outCh, i)
	sendData(conns, kvpair, mapCh)

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMapReduceServiceServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	// if err := s.Serve(lis); err != nil {
	// 	log.Fatalf("failed to serve: %v", err)
	// }
	go s.Serve(lis)

	drumroll := <- outCh

	// fmt.Println(drumroll)

	mapDone = true

	// newInChan := make(chan []string)
	// newOutChan := make(chan map[string]string)

	go doReduction(conns, drumroll, redCh)
	go redAggregate(redCh, newOutChan, len(drumroll))

	ret := <- newOutChan

	fmt.Println(ret)

	allDone = true

}