package main

import (
	"hash/fnv"
	"log"
)

func main() {
	log.Print(ihash("wortest") % 10)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
