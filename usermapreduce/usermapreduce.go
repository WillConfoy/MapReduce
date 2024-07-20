package usermapreduce

import (
	"strconv"
	"fmt"
	"strings"
)

func check(e error) {
    if e != nil {
		fmt.Println("PANIC PANIC PANIC")
        panic(e)
    }
}

func Map(name string, data string) [][]string{
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

func Reduce(key string, vals []string) []string {
	result := 0
	for i := range vals {
		j, err := strconv.Atoi(vals[i])
		check(err)
		result += j
	}
	return []string{key, strconv.Itoa(result)}
}