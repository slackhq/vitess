package vtgateproxy

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

var (
	clientDiscoveryInterval     = flag.Int("client_discovery_interval_seconds", 30, "how often to update the client discovery")
	clientDiscoveryEndpoint     = flag.String("client_discovery_endpoint", "http://rotor-http-dev-us-east-1.internal.ec2.tinyspeck.com:50001/v3/discovery:endpoints", "rotor endpoint to query client list")
	clientDiscoveryResourceType = flag.String("client_discovery_resource_type", "hhvm-metrics@dev-us-east-1", "client resource type")
	clientIndex                 = 0
	clientIndexLock             = &sync.Mutex{}
)

func getWebappDiscovery() (*RotorJson, error) {
	payload := fmt.Sprintf(`{"node":{"cluster":"webapp"}, "resource_names":["%s"]}`, *clientDiscoveryResourceType)
	res, err := http.Post(*clientDiscoveryEndpoint, "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Non-200 response code from rotor: %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := RotorJson{}
	json.Unmarshal(body, &result)
	return &result, nil
}

func getHosts() ([]string, error) {
	discovery, err := getWebappDiscovery()
	if err != nil {
		return nil, err
	}
	nodenames := []string{}
	for _, r := range discovery.Resources {
		for _, e := range r.Endpoints {
			for _, lb := range e.LBEndpoints {
				node := lb.Metadata.FilterMetadata.EnvoyLB
				nodenames = append(nodenames, node.NodeName)
				fmt.Printf("Nodename: %+v\n", node.NodeName)
			}
		}
	}

	return nodenames, nil
}

func setClientIndex() error {
	nodenames, err := getHosts()
	if err != nil {
		return err
	}
	sort.Strings(nodenames)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	for i, n := range nodenames {
		if n == hostname {
			clientIndexLock.Lock()
			defer clientIndexLock.Unlock()
			clientIndex = i
			log.Infof("Client index set to %d", i)
			return nil
		}
	}

	return fmt.Errorf("hostname '%s' not found (client list length %d)", hostname, len(nodenames))
}

func clientDiscovery() {
	ticker := time.NewTicker(time.Duration(*clientDiscoveryInterval) * time.Second)

	for range ticker.C {
		if err := setClientIndex(); err != nil {
			log.Errorf("Failed to set client index: %s", err)
		}
	}
}

func startClientDiscovery() error {
	if err := setClientIndex(); err != nil {
		return err
	}

	go clientDiscovery()

	return nil
}

func getClientIndex() int {
	clientIndexLock.Lock()
	defer clientIndexLock.Unlock()
	return clientIndex
}
