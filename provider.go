package goose

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pressly/goose/v3/internal/migrate"
	"github.com/pressly/goose/v3/internal/sqladapter"
	"github.com/pressly/goose/v3/internal/sqlparser"
	"go.uber.org/multierr"
)

var (
	// ErrVersionNotFound when a migration version is not found.
	ErrVersionNotFound = errors.New("version not found")

	// ErrNoMigration when there are no migrations to apply. Returned by Down and UpByOne.
	ErrNoMigration = errors.New("no migration to apply")

	// ErrAlreadyApplied when a migration has already been applied. Returned by ApplyVersion.
	ErrAlreadyApplied = errors.New("already applied")
)

// Provider is a goose migration provider.
type Provider struct {
	mu         sync.Mutex
	db         *sql.DB
	store      sqladapter.Store
	opt        *ProviderOptions
	migrations []*migrate.Migration
}

// NewProvider returns a new goose Provider.
//
// The caller is responsible for matching the database dialect with the database/sql driver. For
// example, if the database dialect is "postgres", the database/sql driver could be
// github.com/lib/pq or github.com/jackc/pgx.
//
// If opts is nil, the default options are used. See ProviderOptions for more information.
//
// Unless otherwise specified, all methods on Provider are safe for concurrent use.
func NewProvider(dialect Dialect, db *sql.DB, opts *ProviderOptions) (*Provider, error) {
	if db == nil {
		return nil, errors.New("db must not be nil")
	}
	if dialect == "" {
		return nil, errors.New("dialect must not be empty")
	}
	if opts == nil {
		opts = DefaultProviderOptions()
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	store, err := sqladapter.NewStore(string(dialect), opts.Tablename)
	if err != nil {
		return nil, err
	}
	// TODO(mf): avoid using the old collectGoMigrations function and converting between the old
	// migration type and the new migration type.
	collected, err := collectGoMigrations(opts.Filesystem, opts.Dir, registeredGoMigrations, 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	migrations := make([]*migrate.Migration, 0, len(collected))
	for _, c := range collected {
		m, err := convert(c)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, m)
	}
	return &Provider{
		db:         db,
		store:      store,
		opt:        opts,
		migrations: migrations,
	}, nil
}

func convert(old *Migration) (*migrate.Migration, error) {
	m := &migrate.Migration{
		Version:  old.Version,
		Fullpath: old.Source,
	}
	switch filepath.Ext(old.Source) {
	case ".sql":
		m.Type = migrate.TypeSQL
		m.SQLParsed = false
	case ".go":
		m.Type = migrate.TypeGo
		m.Go = &migrate.Go{
			UseTx:      old.UseTx,
			UpFn:       old.UpFnContext,
			DownFn:     old.DownFnContext,
			UpFnNoTx:   old.UpFnNoTxContext,
			DownFnNoTx: old.DownFnNoTxContext,
		}
	default:
		return nil, fmt.Errorf("unknown migration type for source: %s", old.Source)
	}
	return m, nil
}

// MigrationStatus represents the status of a single migration.
type MigrationStatus struct {
	Applied   bool
	AppliedAt time.Time
	Source    Source
}

// StatusOptions represents options for the Status method.
type StatusOptions struct{}

// Status returns the status of all migrations. The returned slice is ordered by ascending version.
//
// If opts is nil, the default options are used. See StatusOptions for more information.
func (p *Provider) Status(ctx context.Context, opts *StatusOptions) ([]*MigrationStatus, error) {
	return nil, errors.New("not implemented")
}

// GetDBVersion returns the max version from the database, regardless of when it was applied. If no
// migrations have been applied, it returns 0.
func (p *Provider) GetDBVersion(ctx context.Context) (int64, error) {
	return -1, errors.New("not implemented")
}

// SourceType represents the type of migration source.
type SourceType string

const (
	// SourceTypeSQL represents a SQL migration.
	SourceTypeSQL SourceType = "sql"
	// SourceTypeGo represents a Go migration.
	SourceTypeGo SourceType = "go"
)

// Source represents a single migration source.
//
// For SQL migrations, Fullpath will always be set. For Go migrations, Fullpath will be set if the
// migration has a corresponding file on disk. It will be empty if a migration was registered
// manually.
type Source struct {
	// Type is the type of migration.
	Type SourceType
	// Full path to the migration file.
	//
	// Example: /path/to/migrations/001_create_users_table.sql
	Fullpath string
	// Version is the version of the migration.
	Version int64
}

// ListSources returns a list of all available migration sources the provider is aware of.
func (p *Provider) ListSources() []*Source {
	var sources []*Source
	for _, m := range p.migrations {
		s := &Source{
			Fullpath: m.Fullpath,
			Version:  m.Version,
		}
		switch m.Type {
		case migrate.TypeSQL:
			s.Type = SourceTypeSQL
		case migrate.TypeGo:
			s.Type = SourceTypeGo
		}
		sources = append(sources, s)
	}
	return sources
}

// Ping attempts to ping the database to verify a connection is available.
func (p *Provider) Ping(ctx context.Context) error {
	return errors.New("not implemented")
}

// Close closes the database connection.
func (p *Provider) Close() error {
	return errors.New("not implemented")
}

// MigrationResult is the result of a migration operation.
//
// Note, the caller is responsible for checking the Error field for any errors that occurred while
// running the migration. If the Error field is not nil, the migration failed.
type MigrationResult struct {
	// Full path to the migration file.
	Fullpath string
	// Version is the parsed version from the migration file name.
	Version int64
	// Duration is the time it took to run the migration.
	Duration time.Duration
	// Direction is the direction the migration was applied (up or down).
	Direction string
	// Empty is true if the file was valid, but no statements to apply in the given direction. These
	// are still tracked as applied migrations, but typically have no effect on the database.
	//
	// For SQL migrations, this means the file contained no statements. For Go migrations, this
	// means the file contained nil up or down functions.
	Empty bool

	// Error is any error that occurred while running the migration.
	Error error
}

// ApplyVersion applies exactly one migration at the specified version. If there is no source for
// the specified version, this method returns ErrNoCurrentVersion. If the migration has been applied
// already, this method returns ErrAlreadyApplied.
//
// If direction is true, the "up" migration is applied. If direction is false, the "down" migration
// is applied.
func (p *Provider) ApplyVersion(ctx context.Context, version int64, direction bool) (*MigrationResult, error) {
	return nil, errors.New("not implemented")
}

// Up applies all new migrations. If there are no new migrations to apply, this method returns empty
// list and nil error.
func (p *Provider) Up(ctx context.Context) ([]*MigrationResult, error) {
	return p.up(ctx, false, math.MaxInt64)
}

// UpByOne applies the next available migration. If there are no migrations to apply, this method
// returns ErrNoMigrations.
func (p *Provider) UpByOne(ctx context.Context) (*MigrationResult, error) {
	return nil, errors.New("not implemented")
}

// UpTo applies all available migrations up to and including the specified version. If there are no
// migrations to apply, this method returns empty list and nil error.
//
// For example, suppose there are 3 new migrations available 9,10,11. The current database version
// is 8 and the requested version is 10. In this scenario only versions 9,10 will be applied.
func (p *Provider) UpTo(ctx context.Context, version int64) ([]*MigrationResult, error) {
	return nil, errors.New("not implemented")
}

// Down rolls back the most recently applied migration. If there are no migrations to apply, this
// method returns ErrNoMigrations.
func (p *Provider) Down(ctx context.Context) (*MigrationResult, error) {
	return nil, errors.New("not implemented")
}

// DownTo rolls back all migrations down to but not including the specified version.
//
// For example, suppose the current database version is 11, and the requested version is 9. In this
// scenario only migrations 11 and 10 will be rolled back.
func (p *Provider) DownTo(ctx context.Context, version int64) ([]*MigrationResult, error) {
	return nil, errors.New("not implemented")
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//

func (p *Provider) up(ctx context.Context, upByOne bool, version int64) (_ []*MigrationResult, retErr error) {
	if version < 1 {
		return nil, fmt.Errorf("version must be a number greater than zero: %d", version)
	}
	conn, cleanup, err := p.initialize(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, cleanup())
	}()
	// If there are no migrations to apply, return early. We run this check after initializing the
	// connection because the version table is lazily created in the initialize function.
	if len(p.migrations) == 0 {
		return nil, nil
	}
	if p.opt.NoVersioning {
		return p.runMigrations(ctx, conn, p.migrations, sqlparser.DirectionUp, upByOne)
	}
	// optimize(mf): Listing all migrations from the database isn't great. This is only required to
	// support the out-of-order (allow missing) feature. For users who don't use this feature, we
	// could just query the database for the current version and then apply migrations that are
	// greater than that version.
	dbMigrations, err := p.store.ListMigrations(ctx, conn)
	if err != nil {
		return nil, err
	}
	if len(dbMigrations) == 0 {
		return nil, fmt.Errorf("expected at least one migration in the database, but found none")
	}
	dbMaxVersion := dbMigrations[0].Version
	// lookupAppliedInDB is a map of all applied migrations in the database.
	lookupAppliedInDB := make(map[int64]bool)
	for _, m := range dbMigrations {
		lookupAppliedInDB[m.Version] = true
	}

	missingMigrations := findMissing(dbMigrations, p.migrations, dbMaxVersion)

	// feature(mf): It is very possible someone may want to apply ONLY new migrations and skip
	// missing migrations entirely. At the moment this is not supported, but leaving this comment
	// because that's where that logic will be handled.
	if len(missingMigrations) > 0 && !p.opt.AllowMissing {
		var collected []string
		for _, v := range missingMigrations {
			collected = append(collected, v.filename)
		}
		msg := "migration"
		if len(collected) > 1 {
			msg += "s"
		}
		return nil, fmt.Errorf("found %d missing (out-of-order) %s: [%s]",
			len(missingMigrations), msg, strings.Join(collected, ","))
	}

	var migrationsToApply []*migrate.Migration
	if p.opt.AllowMissing {
		for _, v := range missingMigrations {
			m, err := p.getMigration(v.versionID)
			if err != nil {
				return nil, err
			}
			migrationsToApply = append(migrationsToApply, m)
		}
	}
	// filter all migrations with a version greater than the supplied version (min) and less than or
	// equal to the requested version (max).
	for _, m := range p.migrations {
		if lookupAppliedInDB[m.Version] {
			continue
		}
		if m.Version > dbMaxVersion && m.Version <= version {
			migrationsToApply = append(migrationsToApply, m)
		}
	}

	// feat(mf): this is where can (optionally) group multiple migrations to be run in a single
	// transaction. The default is to apply each migration sequentially on its own.
	// https://github.com/pressly/goose/issues/222
	//
	// Note, we can't use a single transaction for all migrations because some may have to be run in
	// their own transaction.

	return p.runMigrations(ctx, conn, migrationsToApply, sqlparser.DirectionUp, upByOne)
}

// getMigration returns the migration with the given version. If no migration is found, then
// ErrVersionNotFound is returned.
func (p *Provider) getMigration(version int64) (*migrate.Migration, error) {
	for _, m := range p.migrations {
		if m.Version == version {
			return m, nil
		}
	}
	return nil, ErrVersionNotFound
}

func (p *Provider) ensureVersionTable(ctx context.Context, conn *sql.Conn) (retErr error) {
	// feat(mf): this is where we can check if the version table exists instead of trying to fetch
	// from a table that may not exist. https://github.com/pressly/goose/issues/461
	res, err := p.store.GetMigration(ctx, conn, 0)
	if err == nil && res != nil {
		return nil
	}
	return p.beginTx(ctx, conn, func(tx *sql.Tx) error {
		if err := p.store.CreateVersionTable(ctx, tx); err != nil {
			return err
		}
		if p.opt.NoVersioning {
			return nil
		}
		return p.store.InsertOrDelete(ctx, tx, true, 0)
	})
}

// beginTx begins a transaction and runs the given function. If the function returns an error, the
// transaction is rolled back. Otherwise, the transaction is committed.
//
// If the provider is configured to use versioning, this function also inserts or deletes the
// migration version.
func (p *Provider) beginTx(ctx context.Context, conn *sql.Conn, fn func(tx *sql.Tx) error) (retErr error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *Provider) initialize(ctx context.Context) (*sql.Conn, func() error, error) {
	p.mu.Lock()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		p.mu.Unlock()
		return nil, nil, err
	}
	// cleanup is a function that cleans up the connection, and optionally, the session lock.
	cleanup := func() error { return nil }

	// switch p.opt.LockMode { case LockModeAdvisorySession:
	//  if err := p.store.LockSession(ctx, conn); err != nil {
	//      p.mu.Unlock()
	//      return nil, nil, err
	//  }
	//  cleanup = func() error {
	//      defer p.mu.Unlock()
	//      return errors.Join(p.store.UnlockSession(ctx, conn), conn.Close())
	//  }
	// case LockModeNone:
	//  cleanup = func() error {
	//      defer p.mu.Unlock()
	//      return conn.Close()
	//  }
	// default:
	//  p.mu.Unlock()
	//  return nil, nil, fmt.Errorf("invalid lock mode: %d", p.opt.LockMode)
	// } If versioning is enabled, ensure the version table exists.
	//
	// For ad-hoc migrations, we don't need the version table because there is no versioning.
	if !p.opt.NoVersioning {
		if err := p.ensureVersionTable(ctx, conn); err != nil {
			return nil, nil, multierr.Append(err, cleanup())
		}
	}
	return conn, cleanup, nil
}

// runMigrations runs migrations sequentially in the given direction.
//
// If the migrations slice is empty, this function returns nil with no error.
func (p *Provider) runMigrations(
	ctx context.Context,
	conn *sql.Conn,
	migrations []*migrate.Migration,
	direction sqlparser.Direction,
	byOne bool,
) ([]*MigrationResult, error) {
	if len(migrations) == 0 {
		return nil, nil
	}
	var apply []*migrate.Migration
	if byOne {
		apply = []*migrate.Migration{migrations[0]}
	} else {
		apply = migrations
	}
	// Lazily parse SQL migrations (if any) in both directions. We do this before running any
	// migrations so that we can fail fast if there are any errors and avoid leaving the database in
	// a partially migrated state.
	if err := migrate.ParseSQL(p.opt.Filesystem, p.opt.Debug, apply); err != nil {
		return nil, err
	}

	// TODO(mf): If we decide to add support for advisory locks at the transaction level, this may
	// be a good place to acquire the lock. However, we need to be sure that ALL migrations are safe
	// to run in a transaction.

	//
	//
	//

	// bug(mf): this is a potential deadlock scenario. We're running Go migrations with *sql.DB, but
	// are locking the database with *sql.Conn. If the caller sets max open connections to 1, then
	// this will deadlock because the Go migration will try to acquire a connection from the pool,
	// but the pool is locked.
	//
	// A potential solution is to expose a third Go register function *sql.Conn. Or continue to use
	// *sql.DB and document that the user SHOULD NOT SET max open connections to 1. This is a bit of
	// an edge case. if p.opt.LockMode != LockModeNone && p.db.Stats().MaxOpenConnections == 1 {
	//  for _, m := range apply {
	//      if m.IsGo() && !m.Go.UseTx {
	//          return nil, errors.New("potential deadlock detected: cannot run GoMigrationNoTx with max open connections set to 1")
	//      }
	//  }
	// }

	// Run migrations individually, opening a new transaction for each migration if the migration is
	// safe to run in a transaction.

	// Avoid allocating a slice because we may have a partial migration error. 1. Avoid giving the
	// impression that N migrations were applied when in fact some were not 2. Avoid the caller
	// having to check for nil results
	var results []*MigrationResult
	for _, m := range apply {
		current := &MigrationResult{
			Fullpath:  m.Fullpath,
			Version:   m.Version,
			Direction: strings.ToLower(string(direction)),
			Empty:     m.IsEmpty(direction.ToBool()),
		}

		start := time.Now()
		if err := p.runIndividually(ctx, conn, direction.ToBool(), m); err != nil {
			current.Error = err
			current.Duration = time.Since(start)
			return nil, &PartialError{
				Results: results,
				Failed:  current,
				Err:     err,
			}
		}

		current.Duration = time.Since(start)
		results = append(results, current)
	}
	return results, nil
}

// PartialError is returned when a migration fails, but some migrations already got applied.
type PartialError struct {
	// Results contains the results of all migrations that were applied before the error occurred.
	Results []*MigrationResult
	// Failed contains the result of the migration that failed.
	Failed *MigrationResult
	// Err is the error that occurred while running the migration.
	Err error
}

func (e *PartialError) Error() string {
	var filename string
	if e.Failed != nil {
		filename = fmt.Sprintf("(%s)", filepath.Base(e.Failed.Fullpath))
	} else {
		filename = "(file unknown)"
	}
	return fmt.Sprintf("partial migration error %s: %v", filename, e.Err)
}

// runIndividually runs an individual migration, opening a new transaction if the migration is safe
// to run in a transaction. Otherwise, it runs the migration outside of a transaction with the
// supplied connection.
func (p *Provider) runIndividually(
	ctx context.Context,
	conn *sql.Conn,
	direction bool,
	m *migrate.Migration,
) error {
	if m.UseTx() {
		// Run the migration in a transaction.
		return p.beginTx(ctx, conn, func(tx *sql.Tx) error {
			if err := m.Run(ctx, tx, direction); err != nil {
				return err
			}
			if p.opt.NoVersioning {
				return nil
			}
			return p.store.InsertOrDelete(ctx, tx, direction, m.Version)
		})
	}
	// Run the migration outside of a transaction.
	switch {
	case m.IsGo():
		// Note, we're using *sql.DB instead of *sql.Conn because it's the contract of the
		// GoMigrationNoTx function. This may be a deadlock scenario if the caller sets max open
		// connections to 1. See the comment in runMigrations for more details.
		if err := m.RunNoTx(ctx, p.db, direction); err != nil {
			return err
		}
	case m.IsSQL():
		if err := m.RunConn(ctx, conn, direction); err != nil {
			return err
		}
	}
	if p.opt.NoVersioning {
		return nil
	}
	return p.store.InsertOrDelete(ctx, conn, direction, m.Version)
}
