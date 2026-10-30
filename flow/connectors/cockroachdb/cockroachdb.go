package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type CockroachDBConnector struct {
	*metadataStore.PostgresMetadata
	logger log.Logger
	ssh    *utils.SSHTunnel
	// conn is the connector's own connection, held for the connector's life (a SyncFlow activity's, for CDC).
	// Only one goroutine uses it at a time; connMu only orders ensureConn's swap with the SSH keepalive's close.
	conn   *crdbConn
	connMu sync.Mutex
	// dialConfig is what conn was dialed with, to dial it again
	dialConfig  *pgx.ConnConfig
	config      *protos.CockroachDBConfig
	connStr     string
	crdbVersion string
	// set once the first changefeed session of this connector has released
	// the snapshot history retention job. Never reset: a connector instance
	// lives inside a single SyncFlow activity execution (flowable.go creates
	// it once and calls PullRecords in a loop until the activity ends), so
	// every session it runs resumes from the same flow's stored cursor
	// lineage, which only advances. A genuinely new cursor requires a new
	// SetupReplication (mirror create or resync), and that only happens
	// after the running CDC flow, its SyncFlow activity and therefore this
	// instance are torn down; the fresh instance starts with the flag false.
	// PullFlowCleanup releases the job independently as the backstop.
	historyProtectionChecked atomic.Bool
	// unix nanos of the last snapshot history protection extension fired by
	// this connector, throttling per-partition extension attempts
	historyExtendedAt atomic.Int64
}

func NewCockroachDBConnector(ctx context.Context, env map[string]string, config *protos.CockroachDBConfig) (*CockroachDBConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	flowNameInApplicationName, err := internal.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		logger.Error("Failed to get flow name from application name", slog.Any("error", err))
	}
	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := GetCRDBConnectionString(config, flowName)
	connConfig, err := ParseConfig(connectionString, config)
	if err != nil {
		return nil, err
	}

	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"
	// the snapshot reads INTERVAL columns as text, which follows the session's style; a role can default to another
	connConfig.Config.RuntimeParams["intervalstyle"] = "postgres"

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	tunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, tunnel)
	if err != nil {
		tunnel.Close()
		err = classifyConnectError(err)
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	connector := &CockroachDBConnector{
		PostgresMetadata: pgMetadata,
		logger:           logger,
		config:           config,
		ssh:              tunnel,
		conn:             conn,
		dialConfig:       connConfig,
		connStr:          connectionString,
		crdbVersion:      "",
	}

	tunnel.StartKeepalive(context.Background(), func() {
		connector.logger.Info("SSH keepalive failed, closing connection")
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		connector.connMu.Lock()
		conn := connector.conn
		connector.connMu.Unlock()
		if err := conn.Close(closeCtx); err != nil {
			connector.logger.Error("failed to close CockroachDB connection on SSH keepalive failure", slog.Any("error", err))
		}
	})

	return connector, nil
}

// classifyConnectError wraps authentication and privilege failures in
// exceptions.AuthError like the postgres connector does, so drop flow skips
// source cleanup instead of hammering a peer whose credentials were rotated.
//
// A re-dial during a node failure can get 28000 too, when the node it reaches can't check the credentials yet: that
// is an AuthError as well, which the classifier files under NOTIFY_CONNECTIVITY, not a class that marks the mirror
// STALLED, so a transient one doesn't stall it.
func classifyConnectError(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case pgerrcode.InvalidAuthorizationSpecification,
			pgerrcode.InvalidPassword,
			pgerrcode.InsufficientPrivilege:
			return exceptions.NewAuthError(err)
		}
	}
	return err
}

func (c *CockroachDBConnector) Close() error {
	var errs []error
	if c.conn != nil {
		if err := c.conn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}
	if err := c.ssh.Close(); err != nil {
		c.logger.Error("failed to close SSH tunnel", slog.Any("error", err))
		errs = append(errs, fmt.Errorf("failed to close SSH tunnel: %w", err))
	}
	return errors.Join(errs...)
}

func (c *CockroachDBConnector) ConnectionActive(ctx context.Context) error {
	return c.ensureConn(ctx)
}

// Bounds for ensureConn: a connection whose peer vanished without a reset (a hard node kill, a blackholed route)
// doesn't fail a ping by itself until TCP gives up, many minutes later.
const (
	connPingTimeout = 15 * time.Second
	connDialTimeout = 30 * time.Second
)

// ensureConn checks the connector's own connection and dials it again, once, if it's closed or doesn't answer within
// connPingTimeout. A SyncFlow activity keeps one connector, and so one connection, for its whole life: once the node
// behind it fails, or a load balancer drops it while the mirror is idle, every sync of the activity would fail with
// "connection to source down" until the activity ends, though any other node can serve it. Changefeed sessions dial
// their own connections, so this is about the reads around them: the connection check before each sync, the schema
// read for added columns and the source key read. Call it with no result set open on the connection: a busy
// connection counts as healthy, and isn't replaced.
func (c *CockroachDBConnector) ensureConn(ctx context.Context) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return errors.New("connection is nil")
	}
	pingErr := errors.New("connection closed")
	if !conn.conn.IsClosed() {
		pingCtx, cancel := context.WithTimeout(ctx, connPingTimeout)
		pingErr = conn.Ping(pingCtx)
		cancel()
		if pingErr == nil || strings.Contains(pingErr.Error(), "conn busy") {
			return nil
		}
	}
	if ctx.Err() != nil {
		return errors.Join(ctx.Err(), pingErr)
	}
	dialCtx, cancel := context.WithTimeout(ctx, connDialTimeout)
	defer cancel()
	newConn, err := NewCockroachDBConnFromConfig(dialCtx, c.dialConfig, c.ssh)
	if err != nil {
		// the dial's error leads: it says why the connection can't be restored (e.g. rotated credentials)
		return fmt.Errorf("reconnecting failed: %w (the connection had failed: %v)", classifyConnectError(err), pingErr)
	}
	c.logger.Info("[cockroachdb] reconnected the connector's connection", slog.Any("error", pingErr))
	c.connMu.Lock()
	c.conn = newConn
	c.connMu.Unlock()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	_ = conn.Close(closeCtx)
	return nil
}

func (c *CockroachDBConnector) GetVersion(ctx context.Context) (string, error) {
	if c.crdbVersion != "" {
		return c.crdbVersion, nil
	}
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", fmt.Errorf("failed to get CockroachDB version: %w", err)
	}
	c.crdbVersion = version
	return version, nil
}
