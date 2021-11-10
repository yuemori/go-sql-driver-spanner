package spannerdriver

import (
	"database/sql/driver"
)

// static interface implementation checks of mysqlStmt
var (
	_ driver.Stmt              = &spannerStmt{}
	_ driver.StmtExecContext   = &spannerStmt{}
	_ driver.StmtQueryContext  = &spannerStmt{}
	_ driver.NamedValueChecker = &spannerStmt{}
)
