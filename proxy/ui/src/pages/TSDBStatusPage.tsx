import { useEffect, useState } from 'react'
import { apiFetch } from '../api/client'

interface TSDBStatus {
  numSeries: number
  numSamples: number
  topMetrics: { name: string; seriesCount: number }[]
  topLabelNames: { name: string; seriesCount: number }[]
}

export default function TSDBStatusPage() {
  const [status, setStatus] = useState<TSDBStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    apiFetch('/api/v1/status/tsdb')
      .then((resp: any) => {
        setStatus(resp.data || null)
        setLoading(false)
      })
      .catch(err => {
        setError(err instanceof Error ? err.message : String(err))
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="loading">Loading TSDB status...</div>
  if (error) return <div className="error-box">{error}</div>
  if (!status) return <div className="loading">No data</div>

  return (
    <div>
      <h2 className="page-header">TSDB Status</h2>

      <div className="status-cards">
        <div className="status-card">
          <h3>Total Series</h3>
          <div className="value">{(status.numSeries || 0).toLocaleString()}</div>
        </div>
        <div className="status-card">
          <h3>Total Samples</h3>
          <div className="value">{(status.numSamples || 0).toLocaleString()}</div>
        </div>
      </div>

      {status.topMetrics && status.topMetrics.length > 0 && (
        <>
          <h3 className="section-header">Top Metrics by Series Count</h3>
          <div style={{ overflowX: 'auto', marginBottom: '16px' }}>
            <table>
              <thead>
                <tr><th>Metric Name</th><th>Series Count</th></tr>
              </thead>
              <tbody>
                {status.topMetrics.map(m => (
                  <tr key={m.name}>
                    <td>{m.name}</td>
                    <td>{(m.seriesCount || 0).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {status.topLabelNames && status.topLabelNames.length > 0 && (
        <>
          <h3 className="section-header">Top Label Names by Series Count</h3>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr><th>Label Name</th><th>Series Count</th></tr>
              </thead>
              <tbody>
                {status.topLabelNames.map(l => (
                  <tr key={l.name}>
                    <td>{l.name}</td>
                    <td>{(l.seriesCount || 0).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
