package translator

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func (t *Transpiler) transpileVectorSelector(vs *parser.VectorSelector) (*SQLPlan, error) {
	metricName := vs.Name

	var matchers []LabelMatcher
	for _, m := range vs.LabelMatchers {
		if m.Name == labels.MetricName {
			if metricName == "" {
				metricName = m.Value
			}
			continue
		}
		matchers = append(matchers, LabelMatcher{
			Name: m.Name,
			Op:   m.Type.String(),
			Val:  m.Value,
		})
	}

	stalenessMs := int64(t.cfg.Prometheus.StalenessSeconds) * 1000
	startMs := t.start.UnixMilli()
	endMs := t.end.UnixMilli()
	offsetMs := vs.OriginalOffset.Milliseconds()

	plan := &SQLPlan{
		MetricName:  metricName,
		Matchers:    matchers,
		DataStartMs: startMs - stalenessMs - offsetMs,
		DataEndMs:   endMs - offsetMs,
		ExprType:    "vector",
		AST:         vs.String(),
		Cfg:         t.cfg,
		OffsetMs:    offsetMs,
	}
	return plan, nil
}
