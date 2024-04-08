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

func run(ctx context.Context, st *state, args []string) error {
	root := newRootCommand(st)
	// Add subcommands
	commands := []func(*state) (*ff.Command, error){
		newStatusCommand,
	}
	for _, cmd := range commands {
		c, err := cmd(st)
		if err != nil {
			return err
		}
		root.Subcommands = append(root.Subcommands, c)
	}

	// Parse the flags and return help if requested.
	err := root.Parse(
		args,
		ff.WithEnvVarPrefix("GOOSE"), // Support environment variables for all flags
	)
	if err != nil {
		if errors.Is(err, ff.ErrHelp) {
			fmt.Fprintf(st.stderr, "\n%s\n", createHelp(root))
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
	if err := cmd.Flags.WalkFlags(func(f ff.Flag) error {
		name, ok := f.GetLongName()
		if !ok {
			return fmt.Errorf("flag %v doesn't have a long name", f)
		}
		if requiredFlags[name] && !f.IsSet() {
			required = append(required, "--"+name)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(required) > 0 {
		return fmt.Errorf("required flags not set: %v", strings.Join(required, ", "))
	}
	return nil
}

// func coalesce[T comparable](values ...T) (zero T) {
// 	for _, v := range values {
// 		if v != zero {
// 			return v
// 		}
// 	}
// 	return zero
// }
