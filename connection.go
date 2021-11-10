package spannerdriver

import (
	"context"
	"database/sql/driver"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"github.com/yuemori/go-sql-driver-spanner/internal"
	"google.golang.org/api/iterator"
)

type spannerConn struct {
	client *spanner.Client

	roTx *spanner.ReadOnlyTransaction
	rwTx *rwTx

	closed atomicBool
}

// Prepare implements database/sql/driver.Conn interface
func (c *spannerConn) Prepare(query string) (driver.Stmt, error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	return c.prepare(query)
}

// Close implements database/sql/driver.Conn interface
func (c *spannerConn) Close() error {
	c.cleanup()
	return nil
}

func (c *spannerConn) cleanup() {
	if !c.closed.TrySet(true) {
		return
	}

	c.roTx = nil
	c.rwTx = nil
	c.client = nil
}

// Begin implements database/sql/driver.Conn interface
func (c *spannerConn) Begin() (driver.Tx, error) {
	return nil, notImplementedError(c, "Begin")
}

// BeginTx implements database/sql/driver.ConnBeginTx interface
func (c *spannerConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	if c.inTransaction() {
		return nil, errors.New("already in a transaction")
	}

	if opts.ReadOnly {
		c.roTx = c.client.ReadOnlyTransaction().WithTimestampBound(spanner.StrongRead())
		return &roTx{close: func() {
			c.roTx.Close()
			c.roTx = nil
		}}, nil
	}

	connector := internal.NewRWConnector(ctx, c.client)
	c.rwTx = &rwTx{
		connector: connector,
		close: func() {
			c.rwTx = nil
		},
	}

	// TODO(jbd): Make sure we are not leaking
	// a goroutine in connector if timeout happens.
	select {
	case <-connector.Ready:
		return c.rwTx, nil
	case err := <-connector.Errors: // If received before Ready, transaction failed to start.
		return nil, err
	case <-time.Tick(10 * time.Second):
		return nil, errors.New("cannot begin transaction, timeout after 10 seconds")
	}
	return nil, notImplementedError(c, "BeginTx")
}

// PrepareContext implements database/sql/driver.ConnPrepareContext interface
func (c *spannerConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// ExecContext implements database/sql/driver.ExecerContext interface
func (c *spannerConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

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
	return c.query(ctx, query, args)
}

// Ping implements database/sql/driver.Pinger interface
func (c *spannerConn) Ping(ctx context.Context) error {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}
	if c.inTransaction() {
		return nil
	}

	_, err := c.query(context.Background(), "SELECT 1", []driver.NamedValue{})

	return err
}

// ResetSession implements database/sql/driver.SessionResetter interface
func (c *spannerConn) ResetSession(ctx context.Context) error {
	if c.closed.IsSet() {
		return driver.ErrBadConn
	}
	c.roTx = nil
	c.rwTx = nil

	return nil
}

// IsValid implements database/sql/driver.Validator interface
// (From Go 1.15)
func (c *spannerConn) IsValid() bool {
	return !c.closed.IsSet()
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

func (conn *spannerConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if conn.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

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

func (c *spannerConn) prepare(query string) (*spannerStmt, error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	// TODO(jbd): Mention emails need to be escaped.
	args, err := internal.NamedValueParamNames(query, -1)
	if err != nil {
		return nil, err
	}
	return &spannerStmt{conn: c, query: query, numArgs: len(args)}, nil
}

func (c *spannerConn) inTransaction() bool {
	return c.roTx != nil || c.rwTx != nil
}
