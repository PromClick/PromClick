import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { apiFetch } from '../api/client'

type TabName = 'runtime' | 'config' | 'flags'

const TABS: { key: TabName; label: string }[] = [
  { key: 'runtime', label: 'Runtime' },
  { key: 'config', label: 'Config' },
  { key: 'flags', label: 'Flags' },
]

export default function StatusPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const tabParam = searchParams.get('tab') as TabName | null
  const tab: TabName = tabParam && ['runtime', 'config', 'flags'].includes(tabParam) ? tabParam : 'runtime'

  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const setTab = (t: TabName) => setSearchParams({ tab: t })

  useEffect(() => {
    setLoading(true)
    setError(null)

    const endpoints: Record<TabName, string> = {
      runtime: '/api/v1/status/runtimeinfo',
      config: '/api/v1/status/config',
      flags: '/api/v1/status/flags',
    }

    apiFetch(endpoints[tab])
      .then((resp: any) => {
        setData(resp.data || null)
        setLoading(false)
      })
      .catch(err => {
        setError(err instanceof Error ? err.message : String(err))
        setLoading(false)
      })
  }, [tab])

  return (
    <div>
      <h2 className="page-header">Status</h2>

      <div className="tabs">
        {TABS.map(t => (
          <button
            key={t.key}
            className={`tab ${tab === t.key ? 'active' : ''}`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {error && <div className="error-box">{error}</div>}
      {loading && <div className="loading">Loading...</div>}

      {!loading && data && tab === 'config' && (
        <pre className="config-block">
          {data.yaml || 'No configuration available'}
        </pre>
      )}

      {!loading && data && tab !== 'config' && (
        <div style={{ overflowX: 'auto', marginTop: '1rem' }}>
          <table>
            <thead>
              <tr><th>Key</th><th>Value</th></tr>
            </thead>
            <tbody>
              {Object.entries(data).map(([k, v]) => (
                <tr key={k}>
                  <td>{k}</td>
                  <td style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{String(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
