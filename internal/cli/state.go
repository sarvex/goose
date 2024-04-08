package cli

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// state holds the state of the CLI and is passed to each command. It is used to configure the
// environment, filesystem, and output streams.
type state struct {
	environ []string
	stdout  io.Writer
	stderr  io.Writer
	// This is effectively [fs.SubFS](https://pkg.go.dev/io/fs#SubFS).
	fsys           func(dir string) (fs.FS, error)
	openConnection func(dbstring string) (*sql.DB, goose.Dialect, error)
}

func (s *state) writeJSON(v interface{}) error {
	by, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = s.stdout.Write(by)
	return err
}

func (s *state) initProvider(
	dir string,
	dbstring string,
	tablename string,
	options ...goose.ProviderOption,
) (*goose.Provider, error) {
	if dir == "" {
		return nil, fmt.Errorf("migrations directory is required, set with --dir or GOOSE_DIR")
	}
	if dbstring == "" {
		return nil, errors.New("database connection string is required, set with --dbstring or GOOSE_DBSTRING")
	}
	db, dialect, err := openConnection(dbstring)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	if tablename != "" {
		store, err := database.NewStore(dialect, tablename)
		if err != nil {
			return nil, fmt.Errorf("failed to create store: %w", err)
		}
		options = append(options, goose.WithStore(store))
		// TODO(mf): I don't like how this works. It's not obvious that if a store is provided, the
		// dialect must be set to an empty string. This is because the dialect is set in the store.
		dialect = ""
	}
	fsys, err := s.fsys(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get subtree rooted at dir: %q: %w", dir, err)
	}
	return goose.NewProvider(dialect, db, fsys, options...)
}

var version string

// getVersionFromBuildInfo returns the version string from the build info, if available. It will
// always return a non-empty string.
//
//   - If the build info is not available, it returns "devel".
//   - If the main version is set, it returns the string as is.
//   - If building from source, it returns "devel" followed by the first 12 characters of the VCS
//     revision, followed by ", dirty" if the working directory was dirty. For example,
//     "devel (abcdef012345, dirty)" or "devel (abcdef012345)". If the VCS revision is not available,
//     "unknown revision" is used instead.
//
// Note, vcs info not stamped when built listing .go files directly. E.g.,
//   - `go build main.go`
//   - `go build .`
//
// For more information, see https://github.com/golang/go/issues/51279
func getVersionFromBuildInfo() string {
	if version != "" {
		return version
	}
	const defaultVersion = "devel"

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		// Should only happen if -buildvcs=false is set or using a really old version of Go.
		return defaultVersion
	}
	// The (devel) string is not documented, but it is the value used by the Go toolchain. See
	// https://github.com/golang/go/issues/29228
	if s := buildInfo.Main.Version; s != "" && s != "(devel)" {
		return buildInfo.Main.Version
	}
	var vcs struct {
		revision string
		time     time.Time
		modified bool
	}
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			vcs.revision = setting.Value
		case "vcs.time":
			vcs.time, _ = time.Parse(time.RFC3339, setting.Value)
		case "vcs.modified":
			vcs.modified = (setting.Value == "true")
		}
	}

	var b strings.Builder
	b.WriteString(defaultVersion)
	b.WriteString(" (")
	if vcs.revision == "" || len(vcs.revision) < 12 {
		b.WriteString("unknown revision")
	} else {
		b.WriteString(vcs.revision[:12])
	}
	if vcs.modified {
		b.WriteString(", dirty")
	}
	b.WriteString(")")
	return b.String()
}
