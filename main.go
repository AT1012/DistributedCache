package main

import (
	"fmt"

	"time"

	"github.com/at1012/DistributedCache/cache"
	"github.com/at1012/DistributedCache/consistent_hash"
	"github.com/at1012/DistributedCache/http"
)

func main() {
	fmt.Println("Setting up a Distributed Cache Cluster")
	//Create a cache with ttl or to use default, set it to nil
	c := cache.New(5*time.Minute, -1)

	//Create a consistent hash and set the number of virtual points you want
	//Else take the default of 20
	ch := consistent_hash.New()
	ch.NumberOfReplicas = 50

	//Start the Distributed Cache
	err := http.StartDistributedCacheServer(c, ch, 1, 1, 1)
	panic(err)

}
