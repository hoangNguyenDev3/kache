package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Version information (set during build)
var (
	version   = "1.0.0"
	buildDate = "2023-07-15"
	goVersion = runtime.Version()
)

func init() {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version information",
		Long:  `Print detailed version information about the Redis clone.`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Redis Clone Version: %s\n", version)
			fmt.Printf("Build Date: %s\n", buildDate)
			fmt.Printf("Go Version: %s\n", goVersion)
			fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	}

	rootCmd.AddCommand(versionCmd)
}
