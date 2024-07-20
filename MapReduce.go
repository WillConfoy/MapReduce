package main

import (
	"fmt"
	"os"
	"errors"
	// "math/rand"
	// "net"
	"strings"
	"time"
	"bufio"
	"strconv"
	// user "cs498.com/MapReduce/usermapreduce"
)

type UserMap func(string, string) [][]string
type UserReduce func(string, []string) []string

func accept(conn string) error{
	if conn == "PANIC!!!" {
		return errors.New("panicking")
	}
	return nil
}

func sendData(conns []string, data []string, fn UserMap, inCh chan [][]string) map[string]string {
	// var dataMap map[net.Conn]string
	dataMap := make(map[string]string)
	stringSplit := strings.Split(data[1], "\n")
	for index, element := range stringSplit {
		dataMap[element] = stringSplit[index]
		tempSlice := []string{data[0], element}
		go analyze(conns[index % len(conns)], tempSlice, fn, inCh)
	}
	return dataMap
}

func doReduction(conns []string, data map[string][]string, fn UserReduce, newInChan chan []string) {
	it := 0
	for index, element := range data {
		go doUserReduce(conns[it % len(conns)], index, element, fn, newInChan)
		it++
	}
}

func doUserReduce(conn string, key string, vals []string, reduceFunc UserReduce, newInChan chan []string) {
	err := accept(conn)
	check(err)

	results := reduceFunc(key, vals)

	newInChan <- results
}

func analyze(conn string, data []string, mapFunc UserMap, inCh chan [][]string) {
	err := accept(conn)
	if err != nil {
		fmt.Println("ERROR ERROR ERROR")
		os.Exit(1)
	}

	results := mapFunc(data[0], data[1])

	// fmt.Println(results)

	inCh <- results
}


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

func redAggregate(newInChan chan []string, newOutChan chan map[string]string, num int) {
	bigMap := make(map[string]string)
	for i :=0; i < num; i++{
		newList := <- newInChan
		
		bigMap[newList[0]] = newList[1]

	}

	newOutChan <- bigMap
}


func heartbeats(conns []string, errChannel chan string) {

	for _, element := range conns {
		go heartbeat(element, errChannel)
	}

	for {
		problem := <- errChannel
		fmt.Println("ERROR ERROR ERROR " + problem + " DIED")
		os.Exit(1)
	}
}

func heartbeat(conn string, errChannel chan string) {
	for {
		time.Sleep(1 * time.Second)
		ret := heartbeatResponse(conn)
		if ret != "here" {
			str := conn + " died"
			errChannel <- str
		}
	}
}

func heartbeatResponse(conn string) string{
	return "here"
}

func check(e error) {
    if e != nil {
		fmt.Println("PANIC PANIC PANIC")
        panic(e)
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

func main() {
	// FORMAT OF ARGS: [IP LIST, DATA SOURCE]
	inCh := make(chan [][]string)
	outCh := make(chan map[string][]string)
	errChannel := make(chan string)
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

	
	go heartbeats(args, errChannel)
	userMap := func (name string, data string) [][]string{
		dataNew := strings.Split(data, " ")
		temp := make([][]string, len(dataNew))
		for i := range temp {
			temp[i] = make([]string, 2)
		}

		for i, word := range dataNew {
			temp[i][0] = word
			temp[i][1] = "1"
		}
		return temp
	}

	userReduce := func (key string, vals []string) []string {
		result := 0
		for i := range vals {
			j, err := strconv.Atoi(vals[i])
			check(err)
			result += j
		}
		return []string{key, strconv.Itoa(result)}
	}
	conns := strings.Split(args[0], ",")
	i := <- lengthCh
	go aggregate(inCh, outCh, i)
	sendData(conns, kvpair, userMap, inCh)

	drumroll := <- outCh

	newInChan := make(chan []string)
	newOutChan := make(chan map[string]string)

	go doReduction(conns, drumroll, userReduce, newInChan)
	go redAggregate(newInChan, newOutChan, len(drumroll))

	ret := <- newOutChan

	fmt.Println(ret)

}