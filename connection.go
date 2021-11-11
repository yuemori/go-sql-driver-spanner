package spannerdriver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"github.com/yuemori/go-sql-driver-spanner/internal"
	"google.golang.org/api/iterator"
)

type spannerConn struct {
	client *spanner.Client

	roTx *spanner.ReadOnlyTransaction
	rwTx *spanner.ReadWriteStmtBasedTransaction

	// for context support (Go 1.8+)
	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError // set non-nil if conn is canceled
	closed   atomicBool
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

	close(c.closech)
	if c.roTx != nil {
		c.roTx.Close()
	}

	if c.rwTx != nil {
		c.rwTx.Rollback(context.Background())
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

	if err := c.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer c.finish()

	if c.inTransaction() {
		return nil, errors.New("already in a transaction")
	}

	if opts.ReadOnly {
		c.roTx = c.client.ReadOnlyTransaction().WithTimestampBound(spanner.StrongRead())
		return &roTx{ctx: ctx, conn: c, close: func() {
			c.roTx.Close()
			c.roTx = nil
		}}, nil
	}

	var err error
	c.rwTx, err = spanner.NewReadWriteStmtBasedTransaction(ctx, c.client)
	if err != nil {
		return nil, err
	}

	return &rwTx{ctx: ctx, conn: c, close: func() {
		c.rwTx = nil
	}}, nil
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
	if err := c.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer c.finish()

	if c.roTx != nil {
		return nil, ErrWriteInReadOnlyTransaction
	}
	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	if c.rwTx == nil {
		rowsAffected, err = c.execContextInNewRWTransaction(ctx, ss)
	} else {
		rowsAffected, err = c.rwTx.Update(ctx, ss)
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
func (c *spannerConn) Ping(ctx context.Context) (err error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err = c.watchCancel(ctx); err != nil {
		return
	}
	defer c.finish()

	if c.inTransaction() {
		return nil
	}

	_, err = c.query(context.Background(), "SELECT 1", []driver.NamedValue{})

	return
}

// ResetSession implements database/sql/driver.SessionResetter interface
func (c *spannerConn) ResetSession(ctx context.Context) error {
	if c.closed.IsSet() {
		return driver.ErrBadConn
	}
	if c.roTx != nil {
		c.roTx.Close()
	}
	c.roTx = nil
	if c.rwTx != nil {
		c.rwTx.Rollback(ctx)
	}
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

func (c *spannerConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if err := c.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer c.finish()

	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var it *spanner.RowIterator
	if c.roTx != nil {
		it = c.roTx.Query(ctx, ss)
	} else if c.rwTx != nil {
		it = c.rwTx.Query(ctx, ss)
	} else {
		it = c.client.Single().Query(ctx, ss)
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

// finish is called when the query has canceled.
func (c *spannerConn) cancel(err error) {
	c.canceled.Set(err)
	c.cleanup()
}

func (c *spannerConn) error() error {
	if c.closed.IsSet() {
		if err := c.canceled.Value(); err != nil {
			return err
		}
		return ErrInvalidConn
	}
	return nil
}

// finish is called when the query has succeeded.
func (c *spannerConn) finish() {
	if !c.watching || c.finished == nil {
		return
	}
	select {
	case c.finished <- struct{}{}:
		c.watching = false
	case <-c.closech:
	}
}

func (c *spannerConn) watchCancel(ctx context.Context) error {
	if c.watching {
		// Reach here if canceled,
		// so the connection is already invalid
		c.cleanup()
		return nil
	}
	// When ctx is already cancelled, don't watch it.
	if err := ctx.Err(); err != nil {
		return err
	}
	// When ctx is not cancellable, don't watch it.
	if ctx.Done() == nil {
		return nil
	}
	// When watcher is not alive, can't watch it.
	if c.watcher == nil {
		return nil
	}

	c.watching = true
	c.watcher <- ctx
	return nil
}

func (mc *spannerConn) startWatcher() {
	watcher := make(chan context.Context, 1)
	mc.watcher = watcher
	finished := make(chan struct{})
	mc.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-mc.closech:
				return
			}

			select {
			case <-ctx.Done():
				mc.cancel(ctx.Err())
			case <-finished:
			case <-mc.closech:
				return
			}
		}
	}()
}
