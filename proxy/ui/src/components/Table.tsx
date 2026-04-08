interface VectorItem {
  metric: Record<string, string>
  value: [number, string]
}

interface Props {
  data: VectorItem[]
}

function formatMetric(metric: Record<string, string>): string {
  const name = metric.__name__ || ''
  const labels = Object.entries(metric)
    .filter(([k]) => k !== '__name__')
    .map(([k, v]) => `${k}="${v}"`)
    .join(', ')
  return labels ? `${name}{${labels}}` : name || '{}'
}

export default function Table({ data }: Props) {
  if (!data || data.length === 0) {
    return <div className="loading">No data</div>
  }

  return (
    <div style={{ overflowX: 'auto', marginTop: '0.5rem' }}>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Value</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {data.map((item, i) => (
            <tr key={i}>
              <td>{formatMetric(item.metric)}</td>
              <td>{item.value[1]}</td>
              <td>{new Date(item.value[0] * 1000).toISOString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
