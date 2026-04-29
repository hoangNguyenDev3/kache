package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hoangNguyenDev3/kache/pubsub"
	"github.com/hoangNguyenDev3/kache/server"
	"github.com/hoangNguyenDev3/kache/store"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	// Set a default logger early so packages can log before runServer configures the level
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

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
	serverCmd.Flags().String("aof-fsync", "everysec", "AOF fsync policy (always, everysec, no)")

	// Bind flags to viper
	_ = viper.BindPFlags(serverCmd.Flags()) // viper bind errors are programming errors

	rootCmd.AddCommand(serverCmd)
}

// runServer initializes the Kache store, loads persistence files, starts the
// TCP and HTTP servers, and blocks until an interrupt signal is received.
func runServer(cmd *cobra.Command, args []string) error {
	// Configure slog based on log-level flag
	logLevel := strings.ToLower(viper.GetString("log.level"))
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	slog.Info("starting server", "resp_port", viper.GetInt("resp-port"), "http_port", viper.GetInt("http-port"))

	// Load TLS configuration if enabled
	var tlsConfig *tls.Config
	var certFile, keyFile string
	if viper.GetBool("tls.enabled") {
		certFile = viper.GetString("tls.cert")
		keyFile = viper.GetString("tls.key")
		if certFile == "" || keyFile == "" {
			return errors.New("TLS is enabled but both --tls-cert and --tls-key must be provided")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS key pair: %w", err)
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		slog.Info("TLS enabled", "cert", certFile, "key", keyFile)
	}

	// Initialize store
	storeConfig := &store.StoreConfig{
		AOFPath: viper.GetString("aof-path"),
		RDBPath: viper.GetString("rdb-path"),
	}

	// Create store
	s := store.New(storeConfig)

	// Parse AOF fsync policy
	fsyncStr := viper.GetString("aof-fsync")
	var fsyncPolicy store.FsyncPolicy
	switch fsyncStr {
	case "always":
		fsyncPolicy = store.FsyncAlways
	case "everysec":
		fsyncPolicy = store.FsyncEverySec
	case "no":
		fsyncPolicy = store.FsyncNo
	default:
		fsyncPolicy = store.FsyncEverySec
	}

	// Load data from disk if enabled
	if viper.GetBool("rdb-enabled") {
		if err := s.LoadRDB(viper.GetString("rdb-path")); err != nil {
			slog.Warn("failed to load RDB", "error", err, "path", viper.GetString("rdb-path"))
		}
	}

	if viper.GetBool("aof-enabled") {
		if err := s.LoadAOF(viper.GetString("aof-path")); err != nil {
			slog.Warn("failed to load AOF", "error", err, "path", viper.GetString("aof-path"))
		}
		if err := s.EnableAOF(viper.GetString("aof-path"), fsyncPolicy); err != nil {
			slog.Warn("failed to enable AOF", "error", err, "path", viper.GetString("aof-path"))
		}
	}

	// Start TCP server
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
		TLSConfig:     tlsConfig,
	}
	ps := pubsub.New()
	tcpServer := server.NewTCPServer(s, tcpConfig, ps)
	go func() {
		if err := tcpServer.Start(fmt.Sprintf(":%d", viper.GetInt("resp-port"))); err != nil {
			slog.Error("failed to start TCP server", "error", err)
		}
	}()

	// Start HTTP server
	httpServer := server.NewHTTPServer(s, viper.GetString("auth-token"))
	if tlsConfig != nil {
		httpServer.SetTLS(certFile, keyFile)
	}
	go func() {
		if err := httpServer.Start(fmt.Sprintf(":%d", viper.GetInt("http-port"))); err != nil {
			slog.Error("failed to start HTTP server", "error", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	slog.Info("shutting down")
	if err := tcpServer.Stop(); err != nil {
		slog.Warn("tcp server stop error", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Warn("http server shutdown error", "error", err)
	}

	s.Stop()

	// Save data to disk if enabled
	if viper.GetBool("rdb-enabled") {
		if err := s.SaveRDB(viper.GetString("rdb-path")); err != nil {
			slog.Warn("failed to save RDB", "error", err, "path", viper.GetString("rdb-path"))
		}
	}

	return nil
}
