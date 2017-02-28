package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"os"

	"github.com/at1012/DistributedCache/cache"
	"github.com/at1012/DistributedCache/distributed_cache"
	"github.com/at1012/DistributedCache/serialize_data"
	"github.com/gorilla/mux"
	"stathat.com/c/consistent"
)

var dc *distributed_cache.DistributedCache

func homePageHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "home/")
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]

	//Find in which servers the key resides and do a get request to that server
	servers, err := dc.Ch.GetN(cacheKey, dc.R)
	if err != nil {
		fmt.Errorf("Error getting the server. Err : %#v", err)
		return
	}

	for _, server := range servers {
		url := "http://" + server + "/" + cacheKey
		fmt.Println("\nGET URL:>", url)

		resp, err := http.Get(url)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}

		// form the getKey struct from the response body
		t := serialize_data.GetKey{}
		err = json.NewDecoder(resp.Body).Decode(&t)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		var value interface{}
		if value == nil {
			value = t.Value
			fmt.Fprintf(w, "key: %v  == val: %v == server: %v", t.Key, t.Value, server)
		}

		if value != t.Value {
			//Do read repair
		}

		resp.Body.Close()
	}
}

func getFromCache(w http.ResponseWriter, r *http.Request) {
	var t serialize_data.GetKey
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]
	cacheValue, found := dc.C.Get(cacheKey)

	if found {
		t = serialize_data.GetKey{cacheKey, cacheValue}
	}
	jData, err := json.Marshal(t)
	if err != nil {
		fmt.Errorf("Error marshalling the key, val pair. Err : %#v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jData)
}

func postHandler(w http.ResponseWriter, r *http.Request) {
	t := serialize_data.SaveKey{}
	if r.Body == nil {
		http.Error(w, "Please send a request body", 400)
		return
	}
	err := json.NewDecoder(r.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer r.Body.Close()

	//Get the servers where the key has to be stored into
	servers, err := dc.Ch.GetN(t.Key, dc.W)
	if err != nil {
		fmt.Errorf("Error getting the server. Err : %#v", err)
		return
	}

	for i, server := range servers {
		url := "http://" + server + "/set"
		fmt.Println("\nPOST URL:>", url)

		if i != 0 {
			t.IsReplica = true
			fmt.Fprintf(w, "Saved key: %v  in node %v", t.Key, server)
		}
		jsonStr, err := json.Marshal(t)
		if err != nil {
			fmt.Errorf("Error marshalling the key, value, expiry tuple. Err : %#v", err)
			return
		}
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
	}

}

func saveInCache(w http.ResponseWriter, r *http.Request) {
	t := serialize_data.SaveKey{}
	if r.Body == nil {
		http.Error(w, "Please send a request body", 400)
		return
	}
	err := json.NewDecoder(r.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer r.Body.Close()
	dc.C.Set(t.Key, t.Value, t.Expiry, t.IsReplica)
}

func StartDistributedCacheServer(c *cache.Cache, ch *consistent.Consistent, rf, w, r int) error {
	dc = distributed_cache.New(c, ch, rf, w, r)
	serfAgent, err := dc.SetupCache()
	if err != nil {
		panic(err)
	}

	go func() {
		r1 := mux.NewRouter()
		r1.HandleFunc("/set", saveInCache).Methods("POST")
		r1.HandleFunc("/{cacheKey}", getFromCache).Methods("GET")
		internalPort := os.Args[2]
		http.ListenAndServe(":"+internalPort, r1)
	}()

	r0 := mux.NewRouter()
	r0.HandleFunc("/", homePageHandler)
	r0.HandleFunc("/{cacheKey}", getHandler).Methods("GET")
	r0.HandleFunc("/saveKey", postHandler).Methods("POST")

	DCPort := os.Args[1]
	fmt.Printf("Starting the cache server at port %v", DCPort)
	err = http.ListenAndServe(":"+DCPort, r0)
	if err != nil {
		fmt.Errorf("Error starting the http server. Err : %#v", err)
		err = serfAgent.Wait()
		fmt.Errorf("Error waiting for the asynchronously started serf process. Err : %#v", err)
		return err
	}

	return err

}
