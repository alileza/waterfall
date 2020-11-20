package command

import (
	"github.com/urfave/cli/v2"
)

var GitCommand *cli.Command = &cli.Command{
	Name:        "git",
	Description: "Git related command",
	Usage:       "Git related command",
	Subcommands: []*cli.Command{
		GitSourceCommand,
	},
}
