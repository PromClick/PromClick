package clickhouse

import (
	"fmt"
	"net/url"
	"strconv"
)

// QueryParams buduje URL query params dla CH HTTP interface.
// CH HTTP: {name:Type} w SQL + param_name=value w URL
type QueryParams struct {
	values url.Values
}

func NewParams() *QueryParams {
	return &QueryParams{values: url.Values{}}
}

func (p *QueryParams) AddString(name, val string) string {
	p.values.Set("param_"+name, val)
	return fmt.Sprintf("{%s:String}", name)
}

func (p *QueryParams) AddInt64(name string, val int64) string {
	p.values.Set("param_"+name, strconv.FormatInt(val, 10))
	return fmt.Sprintf("{%s:Int64}", name)
}

func (p *QueryParams) URLValues() url.Values {
	return p.values
}
