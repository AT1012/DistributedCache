package main

import (
	"fmt"

	"github.com/at1012/DistributedCache/http"
)

func main() {
	fmt.Println("Setting up a Distributed Cache Cluster")
	err := http.StartDistributedCacheServer()
	panic(err)

}
