package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/server"
	"github.com/hoangNguyenDev3/redis-clone/store"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start the Redis clone server",
		Long: `Start the Redis clone server with both RESP and HTTP APIs.
Example: redis-clone server --resp-port 6379 --http-port 8080`,
		RunE: runServer,
	}

	// Server flags
	serverCmd.Flags().Int("resp-port", 6379, "RESP protocol port")
	serverCmd.Flags().Int("http-port", 8080, "HTTP API port")
	serverCmd.Flags().String("auth-token", "", "Authentication token for HTTP API")

	// Persistence flags
	serverCmd.Flags().Bool("rdb-enabled", true, "Enable RDB persistence")
	serverCmd.Flags().String("rdb-path", "dump.rdb", "Path to RDB file")
	serverCmd.Flags().Bool("aof-enabled", true, "Enable AOF persistence")
	serverCmd.Flags().String("aof-path", "appendonly.aof", "Path to AOF file")

	// Bind flags to viper
	viper.BindPFlags(serverCmd.Flags())

	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, args []string) error {
	fmt.Printf("Starting server...\n")
	fmt.Printf("RESP port: %d\n", viper.GetInt("resp-port"))
	fmt.Printf("HTTP port: %d\n", viper.GetInt("http-port"))

	// Initialize store
	storeConfig := &store.StoreConfig{
		AOFPath: viper.GetString("aof-path"),
		RDBPath: viper.GetString("rdb-path"),
	}

	// Create store
	s := store.New(storeConfig)

	// Load data from disk if enabled
	if viper.GetBool("rdb-enabled") {
		if err := s.LoadRDB(viper.GetString("rdb-path")); err != nil {
			fmt.Printf("Warning: Failed to load RDB: %v\n", err)
		}
	}

	if viper.GetBool("aof-enabled") {
		if err := s.LoadAOF(viper.GetString("aof-path")); err != nil {
			fmt.Printf("Warning: Failed to load AOF: %v\n", err)
		}
	}

	// Start TCP server
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
	}
	tcpServer := server.NewTCPServer(s, tcpConfig)
	go func() {
		if err := tcpServer.Start(fmt.Sprintf(":%d", viper.GetInt("resp-port"))); err != nil {
			fmt.Printf("Error starting TCP server: %v\n", err)
		}
	}()

	// Start HTTP server
	httpServer := server.NewHTTPServer(s, viper.GetString("auth-token"))
	go func() {
		if err := httpServer.Start(fmt.Sprintf(":%d", viper.GetInt("http-port"))); err != nil {
			fmt.Printf("Error starting HTTP server: %v\n", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fmt.Println("Shutting down...")
	tcpServer.Stop()
	// HTTP server doesn't have a Stop method, so we'll just let it be terminated

	// Save data to disk if enabled
	if viper.GetBool("rdb-enabled") {
		if err := s.SaveRDB(viper.GetString("rdb-path")); err != nil {
			fmt.Printf("Warning: Failed to save RDB: %v\n", err)
		}
	}

	return nil
}
