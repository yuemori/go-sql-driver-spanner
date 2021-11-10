package spannerdriver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/spanner"
	"github.com/yuemori/go-sql-driver-spanner/internal"
)

type spannerStmt struct {
	conn    *spannerConn
	numArgs int
	query   string
}

// Close implements database/sql/driver.Stmt interface.
func (stmt *spannerStmt) Close() error {
	if stmt.conn == nil || stmt.conn.closed.IsSet() {
		return driver.ErrBadConn
	}

	stmt.conn = nil
	return nil
}

// NumInput implements database/sql/driver.Stmt interface.
func (stmt *spannerStmt) NumInput() int {
	return stmt.numArgs
}

// Exec implements database/sql/driver.Stmt interface.
func (stmt *spannerStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, notImplementedError(stmt, "Exec")
}

// Query implements database/sql/driver.Stmt interface.
func (stmt *spannerStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, notImplementedError(stmt, "Query")
}

// ExecContext implements database/sql/driver.StmtExecContext interface.
func (stmt *spannerStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, notImplementedError(stmt, "ExecContext")
}

// QueryContext implements database/sql/driver.StmtQueryContext interface.
func (s *spannerStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.query(ctx, s.query, args)
}

// CheckNamedValue implements database/sql/driver.NamedValueChecker interface.
func (stmt *spannerStmt) CheckNamedValue(*driver.NamedValue) error {
	return notImplementedError(stmt, "CheckNamedValue")
}

func prepareSpannerStmt(q string, args []driver.NamedValue) (spanner.Statement, error) {
	names, err := internal.NamedValueParamNames(q, len(args))
	if err != nil {
		return spanner.Statement{}, err
	}
	ss := spanner.NewStatement(q)
	for i, v := range args {
		name := args[i].Name
		if name == "" {
			name = names[i]
		}
		ss.Params[name] = v.Value
	}
	return ss, nil
}
