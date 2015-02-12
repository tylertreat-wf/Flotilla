package vessel

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

const tcpPort = "7777"

// Broker implements the broker interface for Vessel.
type Broker struct {
	proc *os.Process
}

func (b *Broker) Start(host, port string) (interface{}, error) {
	if port == tcpPort {
		return nil, fmt.Errorf("Port %s is reserved", port)
	}
	cmd := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("vessel --port=%s --tcp-port=%s --http= --sockjs=", port, tcpPort))
	cmd.Stdout = os.Stdout
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	b.proc = cmd.Process
	log.Printf("Started Vessel PID: %d", b.proc.Pid)
	return "", nil
}

func (b *Broker) Stop() (interface{}, error) {
	err := b.proc.Kill()
	if err != nil {
		log.Printf("Failed to stop Vessel PID: %d", b.proc.Pid)
	} else {
		log.Printf("Stopped Vessel PID: %d", b.proc.Pid)
	}
	return "", err
}