package main

import "fmt"

type LogEntry struct {
	Command string
	Term    int
}

func main() {
	log := []LogEntry{} // 日志信息 (start from index 1)
	log = append(log, LogEntry{Term: -1, Command: ""})
	log = append(log, LogEntry{Term: 1, Command: "111"})
	log = append(log, LogEntry{Term: 2, Command: "222"})
	fmt.Println(len(log))
	fmt.Println(log[len(log)-1])
}
