package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/at1012/go-cache"
	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/client"
	"stathat.com/c/consistent"
)

var (
	ch     *consistent.Consistent
	c      *cache.Cache
	s      serfClient
	hostIP string
	err    error
	SELF   string
	SEED   string
)

type serfClient struct {
	rpcClient *client.RPCClient
	mu        sync.RWMutex
}

func homePageHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "home/")
}

type getKey struct {
	Key   string
	Value interface{}
}

//TODO: Give proper err response

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cacheKey := vars["cacheKey"]

	//Find in which server the key resides and do a get request to that server
	server, err := ch.Get(cacheKey)
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
	t := getKey{}
	err = json.NewDecoder(resp.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	fmt.Fprintf(w, "key: %v  == val: %v == server: %v", t.Key, t.Value, server)

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
		fmt.Errorf("Error marshalling the key, val pair. Err : %#v", err)
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

	//Get the server where the key has to be stored into
	server, err := ch.Get(t.Key)
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
	c.Set(t.Key, t.Value, t.Expiry)
}

func setupSerfRPCClient() {

	//Initiate the rpcClient for serf
	serfRPCPort := os.Args[4]
	serfRPCAddr := hostIP + ":" + serfRPCPort
	s.rpcClient, err = client.NewRPCClient(serfRPCAddr)
	if err != nil {
		fmt.Errorf("Create new RPC client failed. Err: %#v", err)
		return
	}

}

func joinCluster() {
	if SELF != SEED {
		_, err := s.rpcClient.Join([]string{SEED}, false)
		if err != nil {
			fmt.Errorf("Error joining the cluster. Err: %#v", err)
			return
		}
	}
	time.Sleep(500 * time.Millisecond)

}

func (s serfClient) getMemberList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	memList := []string{}
	members, err := s.rpcClient.Members()
	if err != nil {
		fmt.Errorf("Error fetching cluster members. Err: %#v", err)
		return nil
	}

	//Add each active node to the newmap
	nodes := []int{}
	for _, mem := range members {
		if mem.Status == "alive" {
			nodes = append(nodes, int(mem.Port))
		}
	}
	//sort.Ints(nodes)

	for _, node := range nodes {
		port := strings.Replace(strconv.Itoa(node), "2", "1", -1)
		memList = append(memList, hostIP+":"+port)
	}

	return memList
}

func getSelfIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Errorf("Error getting the network interfaces. Err: %#v", err)
		return ""
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func main() {
	fmt.Println("Setting up a Distributed Cache Cluster")

	hostIP = getSelfIP()

	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 30 seconds
	c = cache.New(5*time.Minute, 30*time.Second)

	//Start serf agent
	fmt.Println("Starting Serf Agent on this cache server..")

	//TODO: Remove hardcorded value and move this to a script
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
		fmt.Errorf("Error starting the serf agent. Err: %#v", err)
		return
	}
	time.Sleep(500 * time.Millisecond)

	//Set up Serf RPC Client
	setupSerfRPCClient()

	//Setup Seed node
	//Set SEED Node
	//TODO: Remove hardcorded value
	SEED = hostIP + ":7002"
	if SEED == "" {
		//Get the self IP and set yourself as the SEED
	}
	fmt.Println("SEED: ", SEED)

	//Get current IP:Port and if you are not the seed, join the seed node
	SELF = getSelfIP()
	SELF = SELF + ":" + os.Args[3]
	fmt.Println("SELF:", SELF)
	joinCluster()

	//Create consistent hashing instance
	ch = consistent.New()
	for _, n := range s.getMemberList() {
		ch.Add(n)

	}

	//Get active servers list every 1000ms and if it differs from the current list, update cluster info and sync data
	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for {
			select {
			case <-ticker.C:
				newNodeList := s.getMemberList()
				oldNodeList := ch.Members()
				self := getSelfIP() + ":" + os.Args[2]
				for _, oldNode := range oldNodeList {
					// if present in the oldList and not in the newList
					found := false
					for _, newNode := range newNodeList {

						if oldNode == newNode {
							found = true
							break
						}
					}
					if !found {
						//oldNode is dead, remove it from consistent hash circle
						fmt.Printf("\nServer (%v) left the cluster.", oldNode)
						ch.Remove(oldNode) // removes the oldNode and all the virtual nodes of it on the circle.
					}
				}

				for _, newNode := range newNodeList {
					// if present in the newList and not in the oldList
					found := false
					for _, oldNode := range oldNodeList {
						if oldNode == newNode {
							found = true
							break
						}
					}

					if !found { //newNode has newly joined the cluster -- Sync the data
						fmt.Printf("\nServer (%v) joined the cluster.\n", newNode)

						//var nextNodes []string
						nextNodes := make(map[string]interface{})

						//Add the new node to the consistent hash circle.
						ch.Add(newNode) //Adds the virtual nodes for this as well
						time.Sleep(500 * time.Millisecond)

						for i := 0; i < ch.NumberOfReplicas; i++ {
							nextNode, err := ch.Get(strconv.Itoa(i) + newNode)
							if err != nil {
								fmt.Errorf("Error getting the server. Err: %#v", err)
								return
							}
							if _, ok := nextNodes[nextNode]; !ok {
								nextNodes[nextNode] = nil
							}
						}

						for nextNode := range nextNodes {
							fmt.Println("next node: ", nextNode)
							//Get all elements from the next node and rehash them and store again based on the new hash
							//if current node is the next node
							if self == nextNode && newNode != nextNode{
								fmt.Println("Current servers's data may be reshuffled across servers, if necessary..")
								m := c.GetNotExpiredItems()

								//TODO: While it is rehashing, it should not accept post/get requests. Otherwise, it may leave a stale value.
								// This is not handled currently.
								for k, v := range m {
									//Get the hash of the key, find which node it has to be stored into
									server, err := ch.Get(k)
									if err != nil {
										fmt.Errorf("Error getting the server. Err: %#v", err)
										return
									}
									if server != self {
										fmt.Printf("\nKey : %v will be moved to server :%v", k, server)
										url := "http://" + server + "/set"
										fmt.Println("\nPOST URL:>", url)

										t := saveKey{k, v.Object, time.Duration(v.Expiration)}
										jsonStr, err := json.Marshal(t)
										if err != nil {
											fmt.Errorf("Error marshalling the key, value, expiry tuple. Err : %#v", err)
											return
										}
										req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
										req.Header.Set("Content-Type", "application/json")

										client := &http.Client{}
										resp, err := client.Do(req)
										if err != nil {
											panic(err)
										}
										resp.Body.Close()

										//Remove the key from the current server
										c.Delete(k)
									}

								}

							}
						}

						fmt.Println("Done syncing data...")

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
	fmt.Errorf("Error starting the http server. Err : %#v", err)
	return

	err = cmd.Wait()
	fmt.Errorf("Error waiting for the asynchronously started serf process. Err : %#v", err)
	return
	fmt.Println("Done Serving")

}
