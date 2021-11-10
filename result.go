package spannerdriver

import "github.com/pkg/errors"

type spannerResult struct {
	rowsAffected int64
}

// LastInsertId implements database/sql/driver.Result interface.
func (r *spannerResult) LastInsertId() (int64, error) {
	return 0, errors.New("spanner doesn't autogenerate IDs")
}

// LastInsertId implements database/sql/driver.Result interface.
func (r *spannerResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
