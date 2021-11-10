package spannerdriver

import (
	"database/sql/driver"
)

// static interface implementation checks of mysqlStmt
var (
	_ driver.Result = &spannerResult{}
	_ driver.Rows   = &spannerRows{}
	// _ driver.RowsColumnTypeDatabaseTypeName = &spannerRows{}
	// _ driver.RowsColumnTypeLength           = &spannerRows{}
	// _ driver.RowsColumnTypeNullable         = &spannerRows{}
	// _ driver.RowsColumnTypePrecisionScale   = &spannerRows{}
	// _ driver.RowsColumnTypeScanType         = &spannerRows{}
	// _ driver.RowsNextResultSet              = &spannerRows{}
)
