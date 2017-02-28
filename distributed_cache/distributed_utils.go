package distributed_cache

import (
	"fmt"
	"net"
	"os/exec"
	"time"
	"os"
)

func getSelfIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Errorf("Error getting the network interfaces. Err: %#v", err)
		return "", err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("No IPv4 addresses found for this server")
}

func startSerfAgent(hostIP, bindPort, rpcPort string) (*exec.Cmd, error) {
	fmt.Println("Starting Serf Agent on this cache server..")

	//TODO: Remove hardcorded value and move this to a script
	gopath := os.Getenv("path")
	serf := gopath + "/bin/serf"
	bindAddr := hostIP + ":" + bindPort
	rpcAddr := hostIP + ":" + rpcPort
	cmdArgs := []string{"agent",
		"-node=" + bindAddr,
		"-bind=" + bindAddr,
		"-rpc-addr=" + rpcAddr,
	}

	cmd := exec.Command(serf, cmdArgs...)
	err := cmd.Start()
	if err != nil {
		fmt.Errorf("Error starting the serf agent. Err: %#v", err)
		return nil, err
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}
