package spannerdriver

import (
	"database/sql/driver"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type spannerRows struct {
	it *spanner.RowIterator

	colsOnce sync.Once
	done     bool

	dirtyRow   *spanner.Row
	currentRow *spanner.Row
}

// Columns implements database/sql/driver.Rows interface.
func (r *spannerRows) Columns() []string {
	if r.dirtyRow != nil {
		return r.dirtyRow.ColumnNames()
	}
	if r.currentRow != nil {
		return r.currentRow.ColumnNames()
	}

	return []string{}
}

// Close implements database/sql/driver.Rows interface.
func (r *spannerRows) Close() error {
	r.it.Stop()
	return nil
}

// Next implements database/sql/driver.Rows interface.
func (r *spannerRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}

	if r.dirtyRow != nil {
		r.currentRow = r.dirtyRow
		r.dirtyRow = nil
		return r.readRow(dest)
	}

	var err error
	r.currentRow, err = r.it.Next() // returns io.EOF when there is no next
	if err == iterator.Done {
		r.done = true
		return io.EOF
	}
	if err != nil {
		errLog.Print(err)
		return err
	}
	return r.readRow(dest)
}

func (r *spannerRows) readRow(dest []driver.Value) error {
	for i := 0; i < r.currentRow.Size(); i++ {
		var col spanner.GenericColumnValue
		if err := r.currentRow.Column(i, &col); err != nil {
			return err
			dest[i] = col
		}

		switch col.Type.Code {
		case sppb.TypeCode_BOOL:
			var v spanner.NullBool
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Bool
		case sppb.TypeCode_INT64:
			var v spanner.NullInt64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Int64
		case sppb.TypeCode_FLOAT64:
			var v spanner.NullFloat64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Float64
		case sppb.TypeCode_TIMESTAMP:
			var v spanner.NullTime
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Time
		case sppb.TypeCode_DATE:
			var v spanner.NullDate
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.IsNull() {
				dest[i] = v.Date // typed nil
			} else {
				dest[i] = v.Date.In(time.Local) // TODO(jbd): Add note about this.
			}
		case sppb.TypeCode_STRING:
			var v spanner.NullString
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.StringVal
		case sppb.TypeCode_BYTES:
			var v []byte
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v
		case sppb.TypeCode_ARRAY:
			return errors.Errorf("unsupported type: %s", col.Type.Code)
		case sppb.TypeCode_STRUCT:
			return errors.Errorf("unsupported type: %s", col.Type.Code)
		case sppb.TypeCode_NUMERIC:
			return errors.Errorf("unsupported type: %s", col.Type.Code)
		case sppb.TypeCode_JSON:
			return errors.Errorf("unsupported type: %s", col.Type.Code)
		default:
			return errors.Errorf("unsupported type: %s", col.Type.Code)
		}
	}
	return nil
}

// // ColumnTypeDatabaseTypeName implements database/sql/driver.RowsColumnTypeDatabaseTypeName interface.
// func (r *spannerRows) ColumnTypeDatabaseTypeName(index int) string {
// 	return ""
// }

//
// // ColumnTypeLength implements database/sql/driver.RowsColumnTypeLength interface.
// func (r *spannerRows) ColumnTypeLength(index int) (length int64, ok bool) {
// 	return int64(0), false
// }
//
// // ColumnTypeNullable implements database/sql/driver.RowsColumnTypeNullable interface.
// func (r *spannerRows) ColumnTypeNullable(index int) (nullable, ok bool) {
// 	return false, false
// }
//
// // ColumnTypePrecisionScale implements database/sql/driver.RowsColumnTypePrecisionScale interface.
// func (r *spannerRows) ColumnTypePrecisionScale(index int) (presicion, scale int64, ok bool) {
// 	return int64(0), int64(0), false
// }
//
// // ColumnTypeScanType implements database/sql/driver.RowsColumnTypeScanType interface.
// func (r *spannerRows) ColumnTypeScanType(index int) reflect.Type {
// 	return reflect.TypeOf("")
// }
