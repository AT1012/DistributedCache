package distributed_cache

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"time"

	"os"
	"os/exec"

	"fmt"

	"bytes"

	"github.com/at1012/DistributedCache/cache"
	"github.com/at1012/DistributedCache/serialize_data"
	"github.com/hashicorp/serf/client"
	"stathat.com/c/consistent"
)

var (
	hostIP string
	seed   string
	self   string
)

type replica struct {
	replicas []string
	changed  bool
}

type serfClient struct {
	rpcClient *client.RPCClient
	mu        sync.RWMutex
}

func (s *serfClient) setupSerfRPCClient(rpcPort string) error {
	var err error
	serfRPCAddr := hostIP + ":" + rpcPort
	fmt.Println(serfRPCAddr)
	s.rpcClient, err = client.NewRPCClient(serfRPCAddr)
	if err != nil {
		fmt.Errorf("Create new RPC client failed. Err: %#v", err)
		return err
	}
	return nil

}

func (s *serfClient) getMemberList() []string {
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

	for _, node := range nodes {
		port := strings.Replace(strconv.Itoa(node), "2", "1", -1)
		memList = append(memList, hostIP+":"+port)
	}

	return memList
}

func (s *serfClient) joinDCCluster(seed string) error {
	if self != seed {
		_, err := s.rpcClient.Join([]string{seed}, false)
		if err != nil {
			fmt.Errorf("Error joining the cluster. Err: %#v", err)
			return err
		}
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

type DistributedCache struct {
	C       *cache.Cache
	Ch      *consistent.Consistent
	Rf      int
	W       int //Write quorum
	R       int //Read quorum
	N       int //Number of servers in the Distribute Cache cluster
	cluster map[string]replica
	s       *serfClient
	mu      sync.RWMutex
}

func New(c *cache.Cache, ch *consistent.Consistent, rf, w, r int) *DistributedCache {
	dc := new(DistributedCache)
	dc.C = c
	if dc.C == nil {
		// Create a cache with a default expiration time of 5 minutes, and which
		// purges expired items every 30 seconds
		dc.C = cache.New(5*time.Minute, 30*time.Second)
	}

	dc.Ch = ch
	if dc.Ch == nil {
		// Create a default consistent hash with 20 virtual points
		dc.Ch = consistent.New()
	}

	dc.Rf = rf
	if dc.Rf == 0 {
		// Default replication factor = 1
		dc.Rf = 1
	}

	dc.W = w
	if dc.W == 0 || dc.W > dc.Rf {
		dc.W = dc.Rf
	}

	dc.R = r
	if dc.R == 0 || dc.R > dc.W {
		dc.R = dc.W
	}

	dc.cluster = make(map[string]replica)
	dc.s = &serfClient{}
	return dc
}

func (dc *DistributedCache) SetupCache() (*exec.Cmd, error) {
	var err error
	hostIP, err = getSelfIP()
	if err != nil {
		return nil, err
	}

	serfAgent, err := startSerfAgent(hostIP, os.Args[3], os.Args[4])
	if err != nil {
		return nil, err
	}

	err = dc.s.setupSerfRPCClient(os.Args[4])
	if err != nil {
		return nil, err
	}

	//Setup SEED node
	//TODO: Remove hardcorded value
	seed = hostIP + ":7002"
	fmt.Println("SEED: ", seed)

	//Get current IP:Port and if you are not the seed, join the seed node
	self = hostIP + ":" + os.Args[3]
	fmt.Println("SELF:", self)

	err = dc.s.joinDCCluster(seed)
	if err != nil {
		return nil, err
	}

	//Add all the servers to the consistent hash circle
	for _, n := range dc.s.getMemberList() {
		dc.Ch.Add(n)

	}
	//Get active servers list every 1000ms and if it differs from the current list, update cluster info and sync data
	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for {
			select {
			case <-ticker.C:
				newNodeList := dc.s.getMemberList()
				oldNodeList := dc.Ch.Members()

				//TODO: Handle errors in better way
				self, err := getSelfIP()
				if err != nil {
					panic(err)
				}
				self = self + ":" + os.Args[2]
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
						dc.Ch.Remove(oldNode) // removes the oldNode and all the virtual nodes of it on the circle.
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
						dc.Ch.Add(newNode) //Adds the virtual nodes for this as well
						time.Sleep(500 * time.Millisecond)

						for i := 0; i < dc.Ch.NumberOfReplicas; i++ {
							//TODO: Handle errors in better way
							nextNode, err := dc.Ch.Get(strconv.Itoa(i) + newNode)
							if err != nil {
								fmt.Errorf("Error getting the server. Err: %#v", err)
								panic(err)
							}
							if _, ok := nextNodes[nextNode]; !ok {
								nextNodes[nextNode] = nil
							}
						}

						for nextNode := range nextNodes {
							fmt.Println("next node: ", nextNode)
							//Get all elements from the next node and rehash them and store again based on the new hash
							//if current node is the next node
							if self == nextNode && newNode != nextNode {
								fmt.Println("Current servers's data may be reshuffled across servers, if necessary..")
								m := dc.C.GetNotExpiredItems()

								//TODO: While it is rehashing, it should not accept post/get requests. Otherwise, it may leave a stale value.
								// This is not handled currently.
								for k, v := range m {
									//Get the hash of the key, find which node it has to be stored into
									//TODO: Handle errors in better way
									server, err := dc.Ch.Get(k)
									if err != nil {
										fmt.Printf("Error getting the server. Err: %#v", err)
										panic(err)
									}
									if server != self {
										fmt.Printf("\nKey : %v will be moved to server :%v", k, server)
										url := "http://" + server + "/set"
										fmt.Println("\nPOST URL:>", url)

										//TODO: Handle errors in better way
										//TODO: set IsReplica flag
										t := serialize_data.SaveKey{k, v.Object, time.Duration(v.Expiration), false}
										jsonStr, err := json.Marshal(t)
										if err != nil {
											fmt.Printf("Error marshalling the key, value, expiry tuple. Err : %#v", err)
											panic(err)
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
										dc.C.Delete(k)
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

	return serfAgent, nil
}
