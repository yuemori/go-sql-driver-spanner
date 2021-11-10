package spannerdriver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type spannerConn struct {
	client *spanner.Client

	roTx *spanner.ReadOnlyTransaction
	rwTx *rwTx
}

func notImplementedError(receiver interface{}, name string) error {
	return errors.Errorf("%T does not implemented: %s", receiver, name)
}

// Prepare implements database/sql/driver.Conn interface
func (c *spannerConn) Prepare(query string) (driver.Stmt, error) {
	return nil, notImplementedError(c, "Prepare")
}

// Close implements database/sql/driver.Conn interface
func (c *spannerConn) Close() error {
	c.client.Close()
	return nil
}

// Begin implements database/sql/driver.Conn interface
func (c *spannerConn) Begin() (driver.Tx, error) {
	return nil, notImplementedError(c, "Begin")
}

// BeginTx implements database/sql/driver.ConnBeginTx interface
func (c *spannerConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, notImplementedError(c, "BeginTx")
}

// PrepareContext implements database/sql/driver.ConnPrepareContext interface
func (c *spannerConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, notImplementedError(c, "PrepareContext")
}

// ExecContext implements database/sql/driver.ExecerContext interface
func (c *spannerConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.roTx != nil {
		return nil, errors.New("cannot write in read-only transaction")
	}
	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	if c.rwTx == nil {
		rowsAffected, err = c.execContextInNewRWTransaction(ctx, ss)
	} else {
		rowsAffected, err = c.rwTx.ExecContext(ctx, ss)
	}
	if err != nil {
		return nil, err
	}
	return &spannerResult{rowsAffected: rowsAffected}, nil
}

// QueryContext implements database/sql/driver.QueryerContext interface
func (c *spannerConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryContext(ctx, query, args)
}

// Ping implements database/sql/driver.Pinger interface
func (c *spannerConn) Ping(ctx context.Context) error {
	return notImplementedError(c, "Ping")
}

// ResetSession implements database/sql/driver.SessionResetter interface
func (c *spannerConn) ResetSession(ctx context.Context) error {
	return notImplementedError(c, "ResetSession")
}

func (c *spannerConn) execContextInNewRWTransaction(ctx context.Context, statement spanner.Statement) (int64, error) {
	var rowsAffected int64
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		count, err := tx.Update(ctx, statement)
		rowsAffected = count
		return err
	}
	_, err := c.client.ReadWriteTransaction(ctx, fn)
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func (conn *spannerConn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var it *spanner.RowIterator
	if conn.roTx != nil {
		it = conn.roTx.Query(ctx, ss)
	} else if conn.rwTx != nil {
		it = conn.rwTx.Query(ctx, ss)
	} else {
		it = conn.client.Single().Query(ctx, ss)
	}

	row, err := it.Next()
	if err == iterator.Done {
		return &spannerRows{it: it, done: true}, nil
	} else if err != nil {
		return nil, err
	}

	return &spannerRows{dirtyRow: row, it: it, done: false}, nil
}
