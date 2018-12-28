// Package db contains functions and data for inserting vehicle position data into a SQLite database.
package db

import (
	"context"
	"database/sql"
	"github.com/jnschaeffer/gosepta/transitview"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

func newInsertVehicleStmt(db *sql.DB) (*sql.Stmt, error) {
	query := `
INSERT INTO vehicles (
route,
read_time,
label,
vehicle_id,
block_id,
trip,
latitude,
longitude,
direction,
destination,
offset_min,
offset_sec,
heading,
late_min
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	return db.Prepare(query)
}

// Client represents a client for connecting to a SQLite database and inserting vehicle positions.
type Client struct {
	conn       *sql.DB
	insertStmt *sql.Stmt
}

// NewClient creates a new Client.
func NewClient(dbURL string) (*Client, error) {
	conn, errOpen := sql.Open("sqlite3", dbURL)
	if errOpen != nil {
		return nil, errOpen
	}

	out := &Client{
		conn: conn,
	}

	return out, nil
}

// Close closes the underlying client database connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Initialize initializes the database for the client. If the necessary tables already exist, this
// is a no-op.
func (c *Client) Initialize(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS vehicles (
    route TEXT,
    read_time TEXT,
    label TEXT,
    vehicle_id TEXT,
    block_id TEXT,
    trip TEXT,
    latitude REAL,
    longitude REAL,
    direction TEXT,
    destination TEXT,
    offset_min INT,
    offset_sec INT,
    heading INT,
    late_min INT,
    UNIQUE (route, trip, vehicle_id, block_id) ON CONFLICT IGNORE
);`

	_, errExec := c.conn.Exec(query)
	if errExec != nil {
		return errExec
	}

	stmt, errStmt := newInsertVehicleStmt(c.conn)
	if errStmt != nil {
		return errStmt
	}

	c.insertStmt = stmt

	return nil
}

func (c *Client) insertVehiclePosition(stmt *sql.Stmt, t time.Time, route string, pos transitview.VehiclePosition) error {
	timeStr := t.Format("2006-01-02 15:04:05")

	_, errExec := stmt.Exec(
		route,
		timeStr,
		pos.Label,
		pos.VehicleID,
		pos.BlockID,
		pos.Trip,
		pos.Latitude,
		pos.Longitude,
		pos.Direction,
		pos.Destination,
		pos.OffsetMinutes,
		pos.OffsetSeconds,
		pos.Heading,
		pos.LateMinutes,
	)

	return errExec
}

// InsertVehiclePositions inserts the given map of routes to vehicle positions as recorded at the given time into the
// database.
func (c *Client) InsertVehiclePositions(ctx context.Context, t time.Time, ps map[string][]transitview.VehiclePosition) error {
	tx, errTx := c.conn.BeginTx(ctx, nil)
	if errTx != nil {
		return errTx
	}

	var rollback bool

	defer func(tx *sql.Tx) {
		if rollback {
			tx.Rollback()
		}
	}(tx)

	stmt := tx.Stmt(c.insertStmt)

	for route, positions := range ps {
		for _, position := range positions {
			errInsert := c.insertVehiclePosition(stmt, t, route, position)
			if errInsert != nil {
				rollback = true
				return errInsert
			}
		}
	}

	errCommit := tx.Commit()

	return errCommit
}
