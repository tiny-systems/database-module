// Package vectorupsert implements vector_upsert — writes an
// embedding row into a pgvector-enabled Postgres table. Pairs with
// vector_search to form the RAG store half of the agent runtime
// (embed → vector_upsert; later, query embed → vector_search →
// llm_chat).
//
// The component does NOT manage schema. The caller is responsible for
// creating the table and the `CREATE EXTENSION vector;`. The expected
// shape is:
//
//	CREATE TABLE memories (
//	  id        TEXT  PRIMARY KEY,
//	  embedding VECTOR(384),
//	  metadata  JSONB
//	);
//
// Column names are configurable via settings so existing schemas can
// be reused. The metadata column is optional — leave the setting blank
// to skip it entirely.
package vectorupsert

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "vector_upsert"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

// identifierRegex restricts table and column identifiers so we can
// interpolate them into SQL safely. Matches the conservative subset
// allowed by Postgres unquoted identifiers plus optional schema
// prefix.
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

type Context any

type Settings struct {
	EnableErrorPort bool   `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	Table           string `json:"table" required:"true" minLength:"1" title:"Table" description:"Target table. May be schema-qualified (e.g. agents.memories). Must already exist and have the 'vector' extension enabled."`
	IdColumn        string `json:"idColumn" required:"true" minLength:"1" default:"id" title:"Id Column" description:"Primary key column. Used in ON CONFLICT clause."`
	EmbeddingColumn string `json:"embeddingColumn" required:"true" minLength:"1" default:"embedding" title:"Embedding Column" description:"pgvector column name."`
	MetadataColumn  string `json:"metadataColumn" default:"metadata" title:"Metadata Column" description:"JSONB column for arbitrary side data. Leave blank to skip writing metadata."`
}

type Request struct {
	Context   Context        `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN       string         `json:"dsn" title:"DSN" description:"Postgres connection string. Leave empty to use the in-cluster pgvector bundle (auto-discovered); set it to target an external database."`
	Id        string         `json:"id" required:"true" minLength:"1" title:"Id" description:"Primary key. Existing rows with the same id are overwritten."`
	Embedding []float32      `json:"embedding" required:"true" minItems:"1" title:"Embedding" description:"Dense vector — same length as the column dimension."`
	Metadata  map[string]any `json:"metadata,omitempty" configurable:"true" title:"Metadata" description:"Optional JSONB payload (tags, source, timestamps, etc.). Ignored if Metadata Column is blank."`
}

type Response struct {
	Context      Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Id           string  `json:"id" title:"Id"`
	RowsAffected int64   `json:"rowsAffected" title:"Rows Affected"`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			IdColumn:        "id",
			EmbeddingColumn: "embedding",
			MetadataColumn:  "metadata",
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Vector Upsert",
		Info:        "Writes an embedding row into a pgvector table. INSERT ... ON CONFLICT (id) DO UPDATE — existing rows are overwritten. The table, id column, embedding column, and optional metadata column are configurable; the embedding extension must be installed and the table must already exist.",
		Tags:        []string{"Vectors", "Postgres", "pgvector", "RAG", "DB"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.upsert(ctx, handler, in)
}

func (c *Component) upsert(ctx context.Context, handler module.Handler, in Request) module.Result {
	idCol := defaultStr(c.settings.IdColumn, "id")
	embCol := defaultStr(c.settings.EmbeddingColumn, "embedding")
	metaCol := c.settings.MetadataColumn

	if err := validateIdentifier("table", c.settings.Table); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if err := validateIdentifier("id column", idCol); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if err := validateIdentifier("embedding column", embCol); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if metaCol != "" {
		if err := validateIdentifier("metadata column", metaCol); err != nil {
			return c.fail(ctx, handler, in.Context, err)
		}
	}

	// Empty DSN = zero-config path: the in-cluster pgvector bundle the
	// module declared (auto-discovered from env the operator chart
	// injects when the bundle is enabled).
	dsn := in.DSN
	if dsn == "" {
		var derr error
		if dsn, derr = bundle.PostgresDSN("pgvector"); derr != nil {
			return c.fail(ctx, handler, in.Context, derr)
		}
	}
	p, err := pool.Postgres(ctx, dsn)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	vec := formatVector(in.Embedding)

	var sql string
	var args []any

	if metaCol == "" {
		sql = fmt.Sprintf(
			`INSERT INTO %s (%s, %s) VALUES ($1, $2::vector)
			 ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s`,
			c.settings.Table, idCol, embCol,
			idCol, embCol, embCol,
		)
		args = []any{in.Id, vec}
	} else {
		metaBytes, err := json.Marshal(in.Metadata)
		if err != nil {
			return c.fail(ctx, handler, in.Context, fmt.Errorf("encode metadata: %w", err))
		}
		sql = fmt.Sprintf(
			`INSERT INTO %s (%s, %s, %s) VALUES ($1, $2::vector, $3::jsonb)
			 ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s`,
			c.settings.Table, idCol, embCol, metaCol,
			idCol, embCol, embCol, metaCol, metaCol,
		)
		args = []any{in.Id, vec, string(metaBytes)}
	}

	tag, err := p.Exec(ctx, sql, args...)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context:      in.Context,
		Id:           in.Id,
		RowsAffected: tag.RowsAffected(),
	})
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, Error{Context: reqCtx, Error: err.Error()})
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{Name: ResponsePort, Label: "Response", Source: true, Configuration: Response{}, Position: module.Right},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: Error{}, Position: module.Bottom,
	})
}

// formatVector renders []float32 as pgvector's text input format:
// "[v1,v2,v3]". Postgres parses this via the ::vector cast.
func formatVector(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.Grow(len(v) * 12)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

func validateIdentifier(label, ident string) error {
	if !identifierRegex.MatchString(ident) {
		return fmt.Errorf("invalid %s %q: must match [a-zA-Z_][a-zA-Z0-9_]* (optionally schema-qualified)", label, ident)
	}
	return nil
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
