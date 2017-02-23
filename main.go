package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"os"
	"time"

	"net"

	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/client"
	"github.com/patrickmn/go-cache"
	"github.com/stathat/consistent"
)

var (
	ch         consistent.Consistent
	c          *cache.Cache
	s          serfClient
	m          map[int]string
	numServers int
	mChannel   chan map[int]string
	hostIP     string
	err        error
)

type serfClient struct {
	rpcClient *client.RPCClient
	mu        sync.RWMutex
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func homePageHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "home/")
}

type getKey struct {
	Key   string
	Value interface{}
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Inside GetHandler")
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]

	fmt.Println("old list", m)
	newNodeList := s.getMemberList()
	numServers = len(newNodeList)
	m = map[int]string{}
	for k, v := range newNodeList {
		m[k] = v
	}
	fmt.Println("new list", m)

	//Find in which node the key resides and do a get request to that node
	n := int(hash(cacheKey)) % numServers
	server := m[n]
	url := "http://" + server + "/" + cacheKey
	fmt.Println("\nGET URL:>", url)

	resp, err := http.Get(url)
	defer resp.Body.Close()

	// form the getKey struct from the response body
	t := getKey{}
	if r.Body == nil {
		http.Error(w, "Please send a request body", 400)
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer r.Body.Close()
	fmt.Fprintf(w, "key: %v  val: %v", t.Key, t.Value)

}

func getFromCache(w http.ResponseWriter, r *http.Request) {
	var t getKey
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]
	cacheValue, found := c.Get(cacheKey)
	if found {
		t = getKey{cacheKey, cacheValue}
	}
	jData, err := json.Marshal(t)
	if err != nil {
		panic(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jData)
}

type saveKey struct {
	Key    string        `json:"key"`
	Value  interface{}   `json:"value"`
	Expiry time.Duration `json:"expiry"`
}

func postHandler(w http.ResponseWriter, r *http.Request) {
	t := saveKey{}
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

	fmt.Println("old list", m)
	newNodeList := s.getMemberList()
	numServers = len(newNodeList)
	m = map[int]string{}
	for k, v := range newNodeList {
		m[k] = v
	}
	fmt.Println("new list", m)

	//Get the hash of the key, find which node it has to be stored into
	n := int(hash(t.Key)) % numServers
	server := m[n]

	url := "http://" + server + "/set"
	fmt.Println("\nPOST URL:>", url)

	jsonStr, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Fprintf(w, "Saved key: %v  in node %v", t.Key, n)

}

func saveInCache(w http.ResponseWriter, r *http.Request) {
	t := saveKey{}
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
	fmt.Printf("\nSaved key: %v", t.Key)
	c.Set(t.Key, t.Value, t.Expiry)
}

func setupSerfRPCClient() {

	//Initiate the rpcClient for serf
	serfRPCPort := os.Args[4]
	serfRPCAddr := hostIP + ":" + serfRPCPort
	s.rpcClient, err = client.NewRPCClient(serfRPCAddr)
	if err != nil {
		fmt.Printf("%#v", err)
		panic(err)
	}

}

func getSelfAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":" + os.Args[3]
			}
		}
	}
	return ""
}

func joinCluster(SEED string) {
	interfaceAddr := getSelfAddress()
	fmt.Println("SELF Address:", interfaceAddr)
	if interfaceAddr != SEED {
		fmt.Println("SEED: ", SEED)
		num, err := s.rpcClient.Join([]string{SEED}, false)
		if err != nil {
			fmt.Println("Error Joining..: ", num)
			panic(err)
		}
	}

}

func (s serfClient) getMemberList() map[int]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	newMap := map[int]string{}

	members, err := s.rpcClient.Members()
	if err != nil {
		panic(err)
	}

	//Add each active node to the newmap
	nodes := []int{}
	for _, mem := range members {
		if mem.Status == "alive" {
			nodes = append(nodes, int(mem.Port))
		}
	}
	sort.Ints(nodes)

	for i, node := range nodes {
		port := strings.Replace(strconv.Itoa(node), "2", "1", -1)
		newMap[i] = hostIP + ":" + port
		fmt.Printf("\nkey: %v == value : %v", i, newMap[i])
	}

	return newMap
}

func main() {
	fmt.Println("Setting up a Distributed Cache with a default expiration time of 5 minutes and which purges expired items every 30 seconds")
	hostIP = "192.168.41.205"
	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 30 seconds
	c = cache.New(5*time.Minute, 30*time.Second)

	//Create consistent hashing instance
	//ch = consistent.New()

	//Start serf agent

	fmt.Println("Starting Serf Agent on this node..")
	serf := "/Users/Aish/go/myprojects//bin/serf"
	bindAddr := hostIP + ":" + os.Args[3]
	rpcAddr := hostIP + ":" + os.Args[4]
	cmdArgs := []string{"agent",
		"-node=" + bindAddr,
		"-bind=" + bindAddr,
		"-rpc-addr=" + rpcAddr,
	}

	cmd := exec.Command(serf, cmdArgs...)
	err := cmd.Start()
	if err != nil {
		fmt.Println("an error occurred.\n")
		panic(err)
	}
	time.Sleep(500 * time.Millisecond)

	//Set up Serf RPC Client
	setupSerfRPCClient()

	//Setup Seed node
	//Set SEED Node
	SEED := hostIP + ":7002"
	m = map[int]string{0: hostIP + ":7001"}

	if SEED == "" {
		//Get the self IP and set yourself as the SEED
	}

	//Get current IP:Port and if you are not the seed, join the seed node
	joinCluster(SEED)

	////Assuming the serf agents in all nodes are up at {7002,7003}, {8002,8003}, {9002,9003}
	////Get active nodes list every 500s and if it differs from the current list, sync data

	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for {
			select {
			case <-ticker.C:
				fmt.Println("\nSyncing data across nodes.. Ticker ticked  at", ticker.C)
				//get member list
				newNodeList := s.getMemberList()

				if len(newNodeList) != len(m) {
					//sync data
				}

				for k, v := range m {
					// if present in the old map and not in the new map
					v1, ok := newNodeList[k]
					if ok {
						if v1 != v {
							//sync data

						}
					}
				}

			}
		}
	}()

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
	panic(err)

	err = cmd.Wait()
	panic(err)
	fmt.Println("Done Serving")

}
