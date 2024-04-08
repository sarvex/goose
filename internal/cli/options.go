package cli

import (
	"database/sql"
	"fmt"
	"io"
	"io/fs"

	"github.com/pressly/goose/v3"
)

// RunOptions are used to configure the command execution.
type RunOptions interface {
	apply(*state) error
}

type runOptionFunc func(*state) error

func (f runOptionFunc) apply(s *state) error { return f(s) }

// WithEnviron sets the environment variables for the command. This will overwrite the current
// environment, primarily useful for testing.
func WithEnviron(env []string) RunOptions {
	return runOptionFunc(func(s *state) error {
		s.environ = env
		return nil
	})
}

// WithStdout sets the writer for stdout.
func WithStdout(w io.Writer) RunOptions {
	return runOptionFunc(func(s *state) error {
		if w == nil {
			return fmt.Errorf("stdout cannot be nil")
		}
		s.stdout = w
		return nil
	})
}

// WithStderr sets the writer for stderr.
func WithStderr(w io.Writer) RunOptions {
	return runOptionFunc(func(s *state) error {
		if w == nil {
			return fmt.Errorf("stderr cannot be nil")
		}
		s.stderr = w
		return nil
	})
}

// WithFilesystem takes a function that returns a filesystem for the given directory. The directory
// will be the value of the --dir flag passed to the command. A typical use case is to use
// [embed.FS] or [fstest.MapFS]. For example:
//
//	fsys := fstest.MapFS{
//	    "migrations/001_foo.sql": {Data: []byte(`-- +goose Up`)},
//	}
//	err := cli.Run(context.Background(), os.Args[1:], cli.WithFilesystem(fsys.Sub))
//
// The above example will run the command with the filesystem provided by [fsys.Sub].
func WithFilesystem(fsys func(dir string) (fs.FS, error)) RunOptions {
	return runOptionFunc(func(s *state) error {
		if fsys == nil {
			return fmt.Errorf("filesystem cannot be nil")
		}
		if s.fsys != nil {
			return fmt.Errorf("filesystem already set")
		}
		s.fsys = fsys
		return nil
	})
}

// WithOpenConnection sets the function that opens a connection to the database from a DSN string.
// The function should return the dialect and the database connection. The dbstring will typically
// be a DSN, such as "postgres://user:password@localhost/dbname" or "sqlite3://file.db" and it is up
// to the function to parse it.
func WithOpenConnection(open func(dbstring string) (*sql.DB, goose.Dialect, error)) RunOptions {
	return runOptionFunc(func(s *state) error {
		if open == nil {
			return fmt.Errorf("open connection function cannot be nil")
		}
		if s.openConnection != nil {
			return fmt.Errorf("open connection function already set")
		}
		s.openConnection = open
		return nil
	})
}
