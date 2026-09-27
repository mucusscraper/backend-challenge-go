// Package migrations embeds the versioned SQL migrations (goose format) so
// the migrate binary and the integration tests apply exactly the same files.
package migrations

import "embed"

// FS contains every *.sql migration of this directory.
//
//go:embed *.sql
var FS embed.FS
