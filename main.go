package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"waterfall/command"
)

func main() {
	app := &cli.App{
		Name:  "waterfall",
		Usage: "Command line tools to generate events",
		Commands: []*cli.Command{
			command.GitCommand,
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stdout, "ERR: %v\n", err)
		os.Exit(1)
	}
}
