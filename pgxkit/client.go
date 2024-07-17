package pgxkit

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

type pool = pgxpool.Pool

type client struct {
	log           *slog.Logger
	url           string
	opened        bool
	migrations    fs.FS
	migrateAction MigrateActionFlag
	*pool
}

func NewClient(url string, opts ...ClientOption) Client {
	c := client{url: url}
	for _, opt := range opts {
		opt.applyToClient(&c)
	}
	return &c
}

func (c *client) Open(ctx context.Context) error {
	if c.opened {
		return nil
	}

	db, err := Open(ctx, c.url)
	if err != nil {
		return err
	}

	c.pool = db
	c.opened = true

	c.log.Info("migrations", "provided", c.migrations != nil)

	if c.migrations != nil && c.migrateAction.IsSet {
		if err := c.Migrate(ctx, c.migrations, c.migrateAction.Val); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) Conn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := c.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Hijack(), nil
}

type MigrateAction string

const (
	MigrateUp   MigrateAction = "up"
	MigrateDown MigrateAction = "down"
)

const (
	_defaultVersionTable = "public.schema_version"
	_defaultSubtree      = "migrations"
)

func (c *client) hasNestedFS(fsys fs.FS) bool {
	info, err := fs.Stat(fsys, _defaultSubtree)
	return err == nil && info.IsDir()
}

func (c *client) Migrate(ctx context.Context, fsys fs.FS, act MigrateAction) error {
	conn, err := c.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer c.closeConn(ctx, conn)

	if c.hasNestedFS(fsys) {
		fsys, err = fs.Sub(fsys, _defaultSubtree)
		if err != nil {
			return fmt.Errorf("sub migrations directory: %w", err)
		}
	}

	mg, err := migrate.NewMigrator(ctx, conn, _defaultVersionTable)
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}

	if err := mg.LoadMigrations(fsys); err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	if c.log != nil {
		mg.OnStart = func(seq int32, name string, dir string, _ string) {
			c.log.Info("running migration", "sequence", seq, "name", name, "direction", dir)
		}
	}

	switch act {
	case MigrateUp:
		return mg.Migrate(ctx)
	case MigrateDown:
		return mg.MigrateTo(ctx, 0)
	default:
		return fmt.Errorf("invalid migrate action: %s", act)
	}
}

func (c *client) closeConn(ctx context.Context, conn *pgx.Conn) {
	if err := conn.Close(ctx); err != nil {
		c.log.Error("closing connection", slog.Group("error", slog.String("msg", err.Error())))
	}
}

type ClientOption interface {
	applyToClient(*client)
}

type ClientOptionFunc func(*client)

func (f ClientOptionFunc) applyToClient(c *client) { f(c) }

func WithLogger(log *slog.Logger) ClientOptionFunc {
	return func(c *client) { c.log = log }
}

func WithMigrations(fsys fs.FS, act MigrateAction) ClientOptionFunc {
	return func(c *client) {
		c.migrations = fsys
		c.migrateAction = MigrateActionFlag{IsSet: true, Val: act}
	}
}

func ParseMigrateAction(s string) (MigrateAction, error) {
	switch strings.ToLower(s) {
	case "up":
		return MigrateUp, nil
	case "down":
		return MigrateDown, nil
	default:
		return "", fmt.Errorf("invalid migrate action: %s", s)
	}
}

type MigrateActionFlag struct {
	IsSet bool
	Val   MigrateAction
}

func (f *MigrateActionFlag) Set(s string) error {
	act, err := ParseMigrateAction(s)
	if err != nil {
		return err
	}
	f.Val = act
	f.IsSet = true
	return nil
}

func (f *MigrateActionFlag) String() string { return string(f.Val) }
