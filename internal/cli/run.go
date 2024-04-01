package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/peterbourgon/ff/v4"
)

const (
	ENV_NO_COLOR = "NO_COLOR"
)

func run(ctx context.Context, state *state, args []string) error {
	root := newRootCommand(state)
	// Add subcommands
	root.Subcommands = append(root.Subcommands, newStatusCommand(state))

	// Parse the flags and return help if requested.
	if err := root.Parse(args); err != nil {
		if errors.Is(err, ff.ErrHelp) {
			fmt.Fprintf(state.stderr, "\n%s\n", createHelp(root))
			return nil
		}
		return err
	}
	// TODO(mf): ideally this would be done in the ff package. See open issue:
	// https://github.com/peterbourgon/ff/issues/128
	if err := checkRequiredFlags(root); err != nil {
		return err
	}
	return root.Run(ctx)
}

func checkRequiredFlags(cmd *ff.Command) error {
	if cmd != nil {
		cmd = cmd.GetSelected()
	}
	var required []string
	cmd.Flags.WalkFlags(func(f ff.Flag) error {
		name, ok := f.GetLongName()
		if !ok {
			return fmt.Errorf("flag %v doesn't have a long name", f)
		}
		if requiredFlags[name] && !f.IsSet() {
			required = append(required, "--"+name)
		}
		return nil
	})
	if len(required) > 0 {
		return fmt.Errorf("required flags not set: %v", strings.Join(required, ", "))
	}
	return nil
}

func coalesce[T comparable](values ...T) (zero T) {
	for _, v := range values {
		if v != zero {
			return v
		}
	}
	return zero
}
