package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type PodInfo struct {
	Rank       int
	TestRank   string
	SSHCommand string
	GPUIP      string
}

var (
	knownGoodPods = make(chan PodInfo, 10)
)

// getPodIPs gets pod IPs.
func getPodIPs(gpuIPs, sshCommands []string) ([]PodInfo, error) {
	podInfos := make([]PodInfo, len(gpuIPs))
	for i, podIP := range gpuIPs {
		podInfos[i] = PodInfo{GPUIP: podIP, Rank: i, SSHCommand: sshCommands[i]}
	}
	sort.Slice(podInfos, func(i, j int) bool { return podInfos[i].Rank < podInfos[j].Rank })
	return podInfos, nil
}

// pairPods pairs pods into groups of two.
func pairPods(pods []PodInfo) [][]PodInfo {
	var pairs [][]PodInfo
	for i := 0; i < len(pods); i += 2 {
		if i+1 < len(pods) {
			pairs = append(pairs, []PodInfo{pods[i], pods[i+1]})
		} else {
			pairs = append(pairs, []PodInfo{pods[i]})
		}
	}
	return pairs
}

// binarySearchFaultyPods performs a binary search to find faulty pods.
func binarySearchFaultyPods(pods []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(pods) <= 1 {
		return
	}
	mid := len(pods) / 2
	wg.Add(2)
	go testGroup(pods[:mid], wg, faults)
	go testGroup(pods[mid:], wg, faults)
}

// allReduce performs an allReduce operation on a group of pods.
func allReduce(group []PodInfo) (bool, error) {
	if len(group) == 0 {
		return false, errors.New("the group cannot be empty")
	}

	var normalPod PodInfo
	singleNodeTest := len(group) == 1
	minRank := group[0].Rank
	if singleNodeTest {
		group[0].TestRank = strconv.Itoa(0)

		var ok bool
		normalPod, ok := <-knownGoodPods
		if !ok {
			return false, errors.New("failed to retrieve a normal pod from the channel")
		}
		normalPod.TestRank = strconv.Itoa(1)

		group = append(group, normalPod) // 将 normalPod 加入到 group 中
	} else {
		if minRank != 0 {
			for i := range group {
				group[i].TestRank = strconv.Itoa(group[i].Rank % minRank)
			}
		} else {
			for i := range group {
				group[i].TestRank = strconv.Itoa(group[i].Rank)
			}
		}

	}
	masterAddr := group[0].GPUIP
	nNodes := strconv.Itoa(len(group))

	if err := executeSSHCommands(group, masterAddr, nNodes); err != nil {
		if singleNodeTest {
			putBackNormalPod(normalPod)
		}
		return false, err
	}

	for _, pod := range group {
		select {
		case knownGoodPods <- pod:
		default:
			log.Println("Warning: knownGoodPods channel is full or not accepting new entries.")
		}
	}

	if singleNodeTest {
		putBackNormalPod(normalPod)
	}

	return true, nil
}

// executeSSHCommands executes SSH commands for a group of pods.
func executeSSHCommands(group []PodInfo, masterAddr, nNodes string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(group))
	for _, pod := range group {
		wg.Add(1)
		go func(pod PodInfo) {
			defer wg.Done()
			command := fmt.Sprintf("%s 'sudo bash /home/wuxuaner/launchBenchmark.sh %s %s %s'", pod.SSHCommand, masterAddr, nNodes, pod.TestRank)
			if err := runCommandWithTimeout(command, 1*time.Minute); err != nil {
				errChan <- err
			}
		}(pod)
	}
	wg.Wait()
	close(errChan)
	if len(errChan) > 0 {
		return <-errChan
	}
	return nil
}

// runCommandWithTimeout runs a command with a specified timeout.
func runCommandWithTimeout(command string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, "sh", "-c", command).CombinedOutput()
	if err != nil {
		log.Printf("Error executing command: %s, error: %v, output: %s\n", command, err, string(output))
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Command timeout: %s\n", command)
			killCmd := fmt.Sprintf("pkill -f %s", filepath.Base(command))
			if killOutput, killErr := exec.Command("sh", "-c", killCmd).CombinedOutput(); killErr != nil {
				log.Printf("Failed to kill process: %s, error: %v, output: %s\n", killCmd, killErr, string(killOutput))
			}
			return fmt.Errorf("command timed out and process killed: %s", command)
		}
		return err
	}
	log.Printf("Output of command: %s\n%s\n", command, string(output))
	return nil
}

// testGroup tests a group of pods.
func testGroup(group []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(group) == 0 {
		return
	}
	success, err := allReduce(group)
	if !success {
		if len(group) == 1 {
			faults <- group[0].GPUIP
			log.Println("Error in allReduce:", err)
		} else {
			log.Println("Error during allReduce:", err)
			binarySearchFaultyPods(group, wg, faults)
		}
	}
}

// putBackNormalPod puts a normal pod back into the knownGoodPods channel.
func putBackNormalPod(pod PodInfo) {
	select {
	case knownGoodPods <- pod:
	default:
		log.Println("Failed to return normal pod to the channel: Channel is full")
	}
}

func main() {
	sshCommands := []string{
		"ssh -p 40802 wuxuaner@122.115.57.194",
		"ssh -p 40420 wuxuaner@122.115.57.194",
		"ssh -p 40741 wuxuaner@122.115.57.194",
		"ssh -p 41676 wuxuaner@122.115.57.194",
	}

	gpuIPs := []string{
		"10.204.35.156",
		"10.204.38.156",
		"10.204.49.59",
		"10.204.49.244",
	}

	podInfos, err := getPodIPs(gpuIPs, sshCommands)
	if err != nil {
		log.Fatalf("Error getting pod IPs: %v", err)
	}

	pairs := pairPods(podInfos)
	wg := &sync.WaitGroup{}
	faults := make(chan string, len(podInfos))

	for _, pair := range pairs {
		wg.Add(1)
		go testGroup(pair, wg, faults)
	}
	wg.Wait()
	close(faults)

	if len(faults) == 0 {
		log.Println("No faulty pods found. Run the full loop test again.")
		for i := range podInfos {
			podInfos[i].TestRank = strconv.Itoa(podInfos[i].Rank)
			<-knownGoodPods
		}
		if _, err := allReduce(podInfos); err != nil {
			log.Fatalf("Error during allReduce: %v", err)
		} else {
			log.Println("No faulty pods found")
		}
	} else {
		log.Println("All pods have been tested. Faulty pods identification completed.")
		for fault := range faults {
			log.Printf("Faulty pod found: %s\n", fault)
		}
	}
}
