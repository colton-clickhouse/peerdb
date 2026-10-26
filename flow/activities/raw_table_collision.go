package activities

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// CheckSharedRawTable fails a mirror to Postgres whose raw table name Postgres truncates to the same name as another
// mirror's into the same database: they'd share one raw table, and one's raw-batch cleanup would delete the other's
// unnormalized rows. Mirror validation refuses such names; this catches mirrors created before it did. The database
// is compared by host, port and name, since two peers can point at one database.
func CheckSharedRawTable(
	ctx context.Context, catalogPool shared.CatalogPool, flowName string, destinationName string,
) error {
	truncated := func(name string) string {
		raw := connpostgres.RawTableIdentifier(name)
		return raw[:min(len(raw), connpostgres.MaxIdentifierBytes)]
	}
	if len(connpostgres.RawTableIdentifier(flowName)) < connpostgres.MaxIdentifierBytes {
		return nil // only a name Postgres truncates (or one exactly at the limit) can collide
	}
	identity := func(peerName string) (string, error) {
		peer, err := connectors.LoadPeer(ctx, catalogPool, peerName)
		if err != nil {
			return "", err
		}
		pg := peer.GetPostgresConfig()
		if pg == nil {
			return "", nil
		}
		return fmt.Sprintf("%s:%d/%s", pg.Host, pg.Port, pg.Database), nil
	}
	mine, err := identity(destinationName)
	if err != nil || mine == "" {
		return err
	}
	rows, err := catalogPool.Query(ctx, `SELECT f.name, p.name FROM flows f JOIN peers p ON p.id = f.destination_peer
		WHERE p.type = $1 AND f.name <> $2 AND f.query_string IS NULL`, protos.DBType_POSTGRES, flowName)
	if err != nil {
		return fmt.Errorf("failed to list mirrors to Postgres: %w", err)
	}
	type flowPeer struct{ Flow, Peer string }
	others, err := pgx.CollectRows(rows, pgx.RowToStructByPos[flowPeer])
	if err != nil {
		return fmt.Errorf("failed to list mirrors to Postgres: %w", err)
	}
	identities := map[string]string{destinationName: mine}
	for _, other := range others {
		if truncated(other.Flow) != truncated(flowName) {
			continue
		}
		id, ok := identities[other.Peer]
		if !ok {
			if id, err = identity(other.Peer); err != nil {
				return err
			}
			identities[other.Peer] = id
		}
		if id == mine {
			return exceptions.NewSharedRawTableError(flowName, other.Flow, truncated(flowName))
		}
	}
	return nil
}
