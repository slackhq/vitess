package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/guptarohit/asciigraph"
)

var (
	numClients     = flag.Int("c", 9761, "Number of clients")
	numVtgates     = flag.Int("v", 1068, "Number of vtgates")
	numConnections = flag.Int("n", 4, "number of connections per client host")
	numZones       = flag.Int("z", 4, "number of zones")
)

func main() {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	flag.Parse()

	fmt.Printf("Simulating %d clients => %d vtgates with %d zones %d conns per client\n\n",
		*numClients, *numVtgates, *numZones, *numConnections)

	var clients []string
	for i := 0; i < *numClients; i++ {
		clients = append(clients, fmt.Sprintf("client-%03d", i))
	}

	var vtgates []string
	for i := 0; i < *numVtgates; i++ {
		vtgates = append(vtgates, fmt.Sprintf("vtgate-%03d", i))
	}

	// for now just consider 1/N of the s "local"
	localClients := clients[:*numClients / *numZones]
	localVtgates := vtgates[:*numVtgates / *numZones]

	conns := map[string][]string{}

	// Simulate "discovery"
	for _, client := range localClients {
		var clientConns []string

		for i := 0; i < *numConnections; i++ {
			vtgate := localVtgates[rnd.Intn(len(localVtgates))]
			clientConns = append(clientConns, vtgate)
		}

		conns[client] = clientConns
	}

	counts := map[string]int{}
	for _, conns := range conns {
		for _, vtgate := range conns {
			counts[vtgate]++
		}
	}

	histogram := map[int]int{}
	max := 0
	min := -1
	for _, count := range counts {
		histogram[count]++
		if count > max {
			max = count
		}
		if min == -1 || count < min {
			min = count
		}
	}

	fmt.Printf("Conns per vtgate\n%v\n\n", counts)
	fmt.Printf("Histogram of conn counts\n%v\n\n", histogram)

	plot := []float64{}
	for i := 0; i < len(localVtgates); i++ {
		plot = append(plot, float64(counts[localVtgates[i]]))
	}
	sort.Float64s(plot)
	graph := asciigraph.Plot(plot)
	fmt.Println("Number of conns per vtgate host")
	fmt.Println(graph)
	fmt.Println("")
	fmt.Println("")

	fmt.Printf("Conn count per vtgate distribution [%d - %d] (%d clients => %d vtgates with %d zones %d conns\n\n",
		min, max, *numClients, *numVtgates, *numZones, *numConnections)
	plot = []float64{}
	for i := min; i < max; i++ {
		plot = append(plot, float64(histogram[i]))
	}
	graph = asciigraph.Plot(plot)
	fmt.Println(graph)

	fmt.Printf("\nConn stats: min %d max %d spread %d spread/min %f spread/avg %f\n",
		min, max, max-min, float64(max-min)/float64(min), float64(max-min)/float64((max+min)/2))
}
