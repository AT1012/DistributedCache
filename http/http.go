package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"os"

	"github.com/at1012/DistributedCache/distributed_cache"
	"github.com/at1012/DistributedCache/serialize_data"
	"github.com/gorilla/mux"
)

var dc *distributed_cache.DistributedCache

func homePageHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "home/")
}

//TODO: Give proper err response

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]

	//Find in which server the key resides and do a get request to that server
	server, err := dc.Ch.Get(cacheKey)
	if err != nil {
		fmt.Errorf("Error getting the server. Err : %#v", err)
		return
	}

	url := "http://" + server + "/" + cacheKey
	fmt.Println("\nGET URL:>", url)

	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	defer resp.Body.Close()

	// form the getKey struct from the response body
	t := serialize_data.GetKey{}
	err = json.NewDecoder(resp.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	fmt.Fprintf(w, "key: %v  == val: %v == server: %v", t.Key, t.Value, server)

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

	//Get the server where the key has to be stored into
	server, err := dc.Ch.Get(t.Key)
	if err != nil {
		fmt.Errorf("Error getting the server. Err : %#v", err)
		return
	}
	url := "http://" + server + "/set"
	fmt.Println("\nPOST URL:>", url)

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
	defer resp.Body.Close()

	fmt.Fprintf(w, "Saved key: %v  in node %v", t.Key, server)

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
	dc.C.Set(t.Key, t.Value, t.Expiry)
}

func StartDistributedCacheServer() error {
	dc = distributed_cache.New(nil, nil, 2, 2, 2)
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

	r := mux.NewRouter()
	r.HandleFunc("/", homePageHandler)
	r.HandleFunc("/{cacheKey}", getHandler).Methods("GET")
	r.HandleFunc("/saveKey", postHandler).Methods("POST")

	DCPort := os.Args[1]
	fmt.Printf("Starting the cache server at port %v", DCPort)
	err = http.ListenAndServe(":"+DCPort, r)
	if err != nil {
		fmt.Errorf("Error starting the http server. Err : %#v", err)
		err = serfAgent.Wait()
		fmt.Errorf("Error waiting for the asynchronously started serf process. Err : %#v", err)
		return err
	}

	return err

}
