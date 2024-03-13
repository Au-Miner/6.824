package main

import "fmt"

func main() {
	mp := make(map[int]string)
	j, i := mp[1]
	fmt.Println(j)
	fmt.Println(i)
}
