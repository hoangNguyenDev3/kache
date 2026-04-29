// Package cmd provides the Cobra CLI commands for the Kache server.
package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is set at build time via ldflags.
var Version string

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "kache",
		Short: "A production-grade Redis clone in Go",
		Long: `A Redis clone implementing core Redis functionality including:
- In-memory key-value store
- RESP protocol support
- HTTP API
- Persistence (RDB/AOF)
- Monitoring`,
	}
)

// Execute runs the root command and returns any error.
func Execute() error {
	rootCmd.Version = Version
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.redis-clone.yaml)")
	rootCmd.PersistentFlags().String("log-level", "info", "logging level (debug, info, warn, error)")
	rootCmd.PersistentFlags().Bool("tls-enabled", false, "Enable TLS for both servers")
	rootCmd.PersistentFlags().String("tls-cert", "", "Path to TLS certificate file")
	rootCmd.PersistentFlags().String("tls-key", "", "Path to TLS private key file")

	_ = viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))     // viper bind errors are programming errors
	_ = viper.BindPFlag("tls.enabled", rootCmd.PersistentFlags().Lookup("tls-enabled")) // viper bind errors are programming errors
	_ = viper.BindPFlag("tls.cert", rootCmd.PersistentFlags().Lookup("tls-cert"))       // viper bind errors are programming errors
	_ = viper.BindPFlag("tls.key", rootCmd.PersistentFlags().Lookup("tls-key"))         // viper bind errors are programming errors
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".redis-clone")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		slog.Info("using config file", "path", viper.ConfigFileUsed())
	}
}
