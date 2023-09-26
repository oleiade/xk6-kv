// Package kv provides a key-value store extension for k6.
package kv

import (
	"github.com/oleiade/xk6-kv/kv"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/kv", kv.New())
}
