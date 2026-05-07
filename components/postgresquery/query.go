package postgresquery

import (
	"context"
	"fmt"

	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "postgres_query"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

// Row is the user-defined row shape. The user customises the schema in settings
// so downstream edges can navigate $.rows[0].columnName.
type Row map[string]any

func (Row) PrepareJSONSchema(s *jsonschema.Schema) error {
	if len(s.Properties) == 0 {
		s.AdditionalProperties = nil
	}
	return nil
}

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	Row             Row  `json:"row,omitempty" type:"object" title:"Row" description:"Expected shape of each returned row. Sample values are placeholders." configurable:"true"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN     string  `json:"dsn" required:"true" minLength:"1" title:"DSN" description:"Postgres connection string"`
	SQL     string  `json:"sql" required:"true" minLength:"1" title:"SQL" description:"SELECT with $1, $2, ... placeholders" format:"textarea"`
	Params  []any   `json:"params,omitempty" title:"Params"`
}

type Response struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Rows    []Row   `json:"rows" title:"Rows"`
	Count   int     `json:"count" title:"Count"`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Postgres Query",
		Info:        "Runs SELECT against Postgres and returns rows as a list of objects keyed by column name. Configure expected row shape in settings so downstream edges can navigate the result. Connection pool is cached per DSN.",
		Tags:        []string{"Postgres", "SQL", "DB"},
	}
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	if port != RequestPort {
		return fmt.Errorf("unknown port: %s", port)
	}

	in, ok := msg.(Request)
	if !ok {
		return fmt.Errorf("invalid request")
	}
	return c.query(ctx, handler, in)
}

func (c *Component) query(ctx context.Context, handler module.Handler, in Request) any {
	p, err := pool.Postgres(ctx, in.DSN)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	rows, err := p.Query(ctx, in.SQL, in.Params...)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	out := make([]Row, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return c.fail(ctx, handler, in.Context, err)
		}
		row := make(Row, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = values[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Rows:    out,
		Count:   len(out),
	})
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) any {
	if !c.settings.EnableErrorPort {
		return err
	}
	return handler(ctx, ErrorPort, Error{Context: reqCtx, Error: err.Error()})
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{
			Name:   ResponsePort,
			Label:  "Response",
			Source: true,
			Configuration: Response{
				Rows: []Row{c.settings.Row},
			},
			Position: module.Right,
		},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: Error{}, Position: module.Bottom,
	})
}

var (
	_ module.Component    = (*Component)(nil)
	_ jsonschema.Preparer = (*Row)(nil)
)

func init() {
	registry.Register(&Component{})
}
