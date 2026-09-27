// Package migrations incorpora as migrações SQL versionadas (formato goose) para
// que o binário migrate e os testes de integração apliquem exatamente os mesmos arquivos.
package migrations

import "embed"

// FS contém todas as migrações *.sql deste diretório.
//
//go:embed *.sql
var FS embed.FS
