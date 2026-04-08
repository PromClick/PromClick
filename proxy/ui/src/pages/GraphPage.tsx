import { useState, useCallback, useRef } from 'react'
import QueryInput from '../components/QueryInput'
import Graph from '../components/Graph'
import Table from '../components/Table'
import { useQuery } from '../hooks/useQuery'
import { useQueryRange } from '../hooks/useQueryRange'

interface Panel {
  id: number
  expr: string
  tab: 'graph' | 'table'
  timeRange: number
}

const RANGE_OPTIONS = [
  { label: '5m', value: 300 },
  { label: '15m', value: 900 },
  { label: '30m', value: 1800 },
  { label: '1h', value: 3600 },
  { label: '3h', value: 10800 },
  { label: '6h', value: 21600 },
  { label: '12h', value: 43200 },
  { label: '1d', value: 86400 },
]

const MAX_VISIBLE_SERIES = 20

let nextId = 1

function QueryPanel({ panel, onRemove, onChange }: {
  panel: Panel
  onRemove: (id: number) => void
  onChange: (id: number, patch: Partial<Panel>) => void
}) {
  const instantQuery = useQuery()
  const rangeQuery = useQueryRange()
  const [showAll, setShowAll] = useState(false)
  const startTime = useRef(0)
  const [loadTime, setLoadTime] = useState<number | null>(null)

  const execute = useCallback(() => {
    if (!panel.expr.trim()) return
    setShowAll(false)
    setLoadTime(null)
    startTime.current = performance.now()

    const now = Math.floor(Date.now() / 1000)
    const onDone = () => setLoadTime(performance.now() - startTime.current)

    if (panel.tab === 'graph') {
      const start = (now - panel.timeRange).toString()
      const end = now.toString()
      const step = Math.max(15, Math.floor(panel.timeRange / 250)).toString()
      rangeQuery.execute(panel.expr, start, end, step).then(onDone)
    } else {
      instantQuery.execute(panel.expr, now.toString()).then(onDone)
    }
  }, [panel.expr, panel.tab, panel.timeRange, instantQuery, rangeQuery])

  const rangeResult = rangeQuery.data as { data?: { result?: { metric: Record<string, string>; values: [number, string][] }[] } } | null
  const instantResult = instantQuery.data as { data?: { result?: { metric: Record<string, string>; value: [number, string] }[] } } | null

  const allRangeSeries = rangeResult?.data?.result || []
  const allInstantSeries = instantResult?.data?.result || []
  const totalSeries = panel.tab === 'graph' ? allRangeSeries.length : allInstantSeries.length
  const visibleRangeSeries = showAll ? allRangeSeries : allRangeSeries.slice(0, MAX_VISIBLE_SERIES)
  const visibleInstantSeries = showAll ? allInstantSeries : allInstantSeries.slice(0, MAX_VISIBLE_SERIES)
  const isTruncated = totalSeries > MAX_VISIBLE_SERIES && !showAll

  return (
    <div className="query-panel">
      <div className="query-panel-toolbar">
        <select
          value={panel.timeRange}
          onChange={e => onChange(panel.id, { timeRange: Number(e.target.value) })}
        >
          {RANGE_OPTIONS.map(o => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>

        <div className="query-input-wrapper">
          <QueryInput
            value={panel.expr}
            onChange={v => onChange(panel.id, { expr: v })}
            onExecute={execute}
          />
        </div>

        <button className="btn btn-execute" onClick={execute}>Execute</button>
        <button className="btn btn-remove" onClick={() => onRemove(panel.id)} title="Remove panel">&times;</button>
      </div>

      <div className="tabs">
        <button
          className={`tab ${panel.tab === 'graph' ? 'active' : ''}`}
          onClick={() => onChange(panel.id, { tab: 'graph' })}
        >
          Graph
        </button>
        <button
          className={`tab ${panel.tab === 'table' ? 'active' : ''}`}
          onClick={() => onChange(panel.id, { tab: 'table' })}
        >
          Table
        </button>

        {/* Stats bar — right side of tabs */}
        {(loadTime !== null || totalSeries > 0) && (
          <div className="query-stats">
            {loadTime !== null && (
              <span className="query-stat">
                Load time: <strong>{loadTime < 1000 ? `${Math.round(loadTime)}ms` : `${(loadTime / 1000).toFixed(2)}s`}</strong>
              </span>
            )}
            {totalSeries > 0 && (
              <span className="query-stat">
                Result series: <strong>{showAll ? totalSeries : Math.min(totalSeries, MAX_VISIBLE_SERIES)}{!showAll && totalSeries > MAX_VISIBLE_SERIES ? ` / ${totalSeries}` : ''}</strong>
              </span>
            )}
          </div>
        )}
      </div>

      {(rangeQuery.error || instantQuery.error) && (
        <div className="error-box">{rangeQuery.error || instantQuery.error}</div>
      )}

      {(rangeQuery.loading || instantQuery.loading) && <div className="loading">Loading...</div>}

      {panel.tab === 'graph' && visibleRangeSeries.length > 0 && (
        <Graph data={visibleRangeSeries} />
      )}

      {panel.tab === 'table' && visibleInstantSeries.length > 0 && (
        <Table data={visibleInstantSeries} />
      )}

      {isTruncated && (
        <button className="btn-show-all" onClick={() => setShowAll(true)}>
          Showing {MAX_VISIBLE_SERIES} of {totalSeries} series — Show all
        </button>
      )}
      {showAll && totalSeries > MAX_VISIBLE_SERIES && (
        <button className="btn-show-all" onClick={() => setShowAll(false)}>
          Showing all {totalSeries} series — Collapse to {MAX_VISIBLE_SERIES}
        </button>
      )}
    </div>
  )
}

export default function GraphPage() {
  const [panels, setPanels] = useState<Panel[]>([
    { id: nextId++, expr: '', tab: 'graph', timeRange: 3600 },
  ])

  const addPanel = () =>
    setPanels(p => [...p, { id: nextId++, expr: '', tab: 'graph', timeRange: 3600 }])

  const removePanel = (id: number) =>
    setPanels(p => (p.length > 1 ? p.filter(x => x.id !== id) : p))

  const updatePanel = (id: number, patch: Partial<Panel>) =>
    setPanels(p => p.map(x => (x.id === id ? { ...x, ...patch } : x)))

  return (
    <div>
      {panels.map(p => (
        <QueryPanel key={p.id} panel={p} onRemove={removePanel} onChange={updatePanel} />
      ))}
      <button className="btn-add-panel" onClick={addPanel}>
        + Add Panel
      </button>
    </div>
  )
}
