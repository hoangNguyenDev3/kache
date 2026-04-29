package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hoangNguyenDev3/kache/cmd"
)

var Version = "dev"

func main() {
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cmd.Version = Version
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
