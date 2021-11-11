package spannerdriver

import "sync/atomic"

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// atomicBool is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type atomicBool struct {
	_noCopy noCopy
	value   uint32
}

// IsSet returns whether the current boolean value is true
func (ab *atomicBool) IsSet() bool {
	return atomic.LoadUint32(&ab.value) > 0
}

// Set sets the value of the bool regardless of the previous value
func (ab *atomicBool) Set(value bool) {
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// TrySet sets the value of the bool and returns whether the value changed
func (ab *atomicBool) TrySet(value bool) bool {
	if value {
		return atomic.SwapUint32(&ab.value, 1) == 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}

// atomicError is a wrapper for atomically accessed error values
type atomicError struct {
	_noCopy noCopy
	value   atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (ae *atomicError) Set(value error) {
	ae.value.Store(value)
}

// Value returns the current error value
func (ae *atomicError) Value() error {
	if v := ae.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}
