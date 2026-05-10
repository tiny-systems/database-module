package postgresexec

import (
	"context"
	"fmt"

	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "postgres_exec"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN     string  `json:"dsn" required:"true" minLength:"1" title:"DSN" description:"Postgres connection string (e.g. postgres://user:pass@host:port/db?sslmode=disable)"`
	SQL     string  `json:"sql" required:"true" minLength:"1" title:"SQL" description:"INSERT/UPDATE/DELETE with $1, $2, ... placeholders" format:"textarea"`
	Params  []any   `json:"params,omitempty" title:"Params" description:"Positional parameters for $1, $2, ..."`
}

type Response struct {
	Context      Context `json:"context,omitempty" configurable:"true" title:"Context"`
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
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Postgres Exec",
		Info:        "Executes INSERT/UPDATE/DELETE against Postgres with positional parameters. Connection pool is cached per DSN across calls.",
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
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.run(ctx, handler, in)
}

func (c *Component) run(ctx context.Context, handler module.Handler, in Request) module.Result {
	p, err := pool.Postgres(ctx, in.DSN)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	tag, err := p.Exec(ctx, in.SQL, in.Params...)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context:      in.Context,
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

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
