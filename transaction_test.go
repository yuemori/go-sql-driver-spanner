package spannerdriver

import (
	"database/sql/driver"
)

// static interface implementation checks of mysqlStmt
var (
	_ driver.Tx = &rwTx{}
	_ driver.Tx = &roTx{}
)
