package spannerdriver

import (
	"database/sql/driver"
)

// static interface implementation checks of spannerConn
var (
	_ driver.Conn               = &spannerConn{}
	_ driver.ConnBeginTx        = &spannerConn{}
	_ driver.ConnPrepareContext = &spannerConn{}
	_ driver.ExecerContext      = &spannerConn{}
	_ driver.QueryerContext     = &spannerConn{}
	_ driver.Pinger             = &spannerConn{}
	_ driver.SessionResetter    = &spannerConn{}
)
