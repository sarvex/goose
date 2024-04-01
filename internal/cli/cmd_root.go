package cli

import (
	"context"
	"fmt"

	"github.com/peterbourgon/ff/v4"
)

type cmdRoot struct {
	state *state
	fs    *ff.FlagSet

	// flags
	version bool
}

func newRootCommand(state *state) *ff.Command {
	c := &cmdRoot{
		state: state,
		fs:    ff.NewFlagSet("goose"),
	}
	c.fs.BoolVarDefault(&c.version, 0, "version", false, "print version and exit")

	cmd := &ff.Command{
		Name:      "goose",
		Usage:     "goose <command> [flags] [args...]",
		ShortHelp: "A database migration tool. Supports SQL migrations and Go functions.",
		Flags:     c.fs,
		Exec:      c.Exec,
	}
	return cmd
}

func (c *cmdRoot) Exec(ctx context.Context, args []string) error {
	if c.version {
		fmt.Fprintf(c.state.stdout, "goose version: %s\n", getVersionFromBuildInfo())
		return nil
	}
	return nil
}
