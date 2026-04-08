import { useEffect, useRef, useState } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'

interface Series {
  metric: Record<string, string>
  values: [number, string][]
}

interface Props {
  data: Series[]
}

const COLORS = [
  '#228be6', '#40c057', '#ae3ec9', '#fa5252',
  '#f76707', '#15aabf', '#7950f2', '#e64980',
]

function buildLabel(metric: Record<string, string>): string {
  const name = metric.__name__ || ''
  const labels = Object.entries(metric)
    .filter(([k]) => k !== '__name__')
    .map(([k, v]) => `${k}="${v}"`)
    .join(', ')
  return labels ? `${name}{${labels}}` : name || '{}'
}

function formatValue(v: number | null | undefined): string {
  if (v == null || isNaN(v)) return 'N/A'
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(2) + 'G'
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(2) + 'M'
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(2) + 'k'
  if (Math.abs(v) < 0.001 && v !== 0) return v.toExponential(3)
  return v.toPrecision(6)
}

interface TooltipInfo {
  x: number
  y: number
  timestamp: string
  closest: { name: string; value: string; color: string; metric: Record<string, string> } | null
}

export default function Graph({ data }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)

  useEffect(() => {
    if (!chartRef.current || !containerRef.current || !data || data.length === 0) return

    const tsSet = new Set<number>()
    data.forEach(s => s.values.forEach(([t]) => tsSet.add(t)))
    const timestamps = Array.from(tsSet).sort((a, b) => a - b)

    const tsIndex = new Map<number, number>()
    timestamps.forEach((t, i) => tsIndex.set(t, i))

    const plotData: (number | null)[][] = [timestamps]
    const series: uPlot.Series[] = [{}]

    data.forEach((s, idx) => {
      const vals = new Array<number | null>(timestamps.length).fill(null)
      s.values.forEach(([t, v]) => {
        const i = tsIndex.get(t)
        if (i !== undefined) vals[i] = parseFloat(v)
      })
      plotData.push(vals)
      series.push({
        label: `s${idx + 1}`,
        stroke: COLORS[idx % COLORS.length],
        width: 1.5,
        show: true,
      })
    })

    // Tooltip plugin
    const tooltipPlugin = {
      hooks: {
        setCursor: (u: uPlot) => {
          const idx = u.cursor.idx
          if (idx == null || idx < 0 || idx >= timestamps.length) {
            setTooltip(null)
            return
          }

          const ts = timestamps[idx]
          const tsStr = new Date(ts * 1000).toISOString()

          // Find closest series to cursor Y
          const cx = u.cursor.left ?? 0
          const cy = u.cursor.top ?? 0

          if (cx < 0 || cy < 0) { setTooltip(null); return }

          let closestIdx = -1
          let closestDist = Infinity

          for (let si = 1; si < u.series.length; si++) {
            const val = (plotData[si] as (number | null)[])[idx]
            if (val == null) continue
            const py = u.valToPos(val, 'y', true)
            const dist = Math.abs(cy - py)
            if (dist < closestDist) {
              closestDist = dist
              closestIdx = si - 1
            }
          }

          const rect = chartRef.current?.getBoundingClientRect()
          if (!rect) { setTooltip(null); return }

          const closestInfo = closestIdx >= 0 ? {
            name: buildLabel(data[closestIdx].metric),
            value: formatValue((plotData[closestIdx + 1] as (number | null)[])[idx]),
            color: COLORS[closestIdx % COLORS.length],
            metric: data[closestIdx].metric,
          } : null

          setTooltip({
            x: cx + rect.left + window.scrollX,
            y: rect.top + window.scrollY + 20,
            timestamp: tsStr,
            closest: closestInfo,
          })
        },
      },
    }

    // Use full width of chart div (subtract padding of parent)
    const chartWidth = chartRef.current.parentElement?.clientWidth
      ? chartRef.current.parentElement.clientWidth - 2  // minus border
      : chartRef.current.clientWidth || 800

    const opts: uPlot.Options = {
      width: chartWidth,
      height: 300,
      padding: [16, 16, 0, 0],
      series,
      legend: { show: false },
      plugins: [tooltipPlugin],
      axes: [
        {
          stroke: '#868e96',
          grid: { stroke: 'rgba(255,255,255,0.05)', width: 1 },
          ticks: { stroke: 'rgba(255,255,255,0.08)', width: 1, size: 4 },
          font: '11px DejaVu Sans Mono, Menlo, Consolas, monospace',
          gap: 8,
        },
        {
          stroke: '#868e96',
          grid: { stroke: 'rgba(255,255,255,0.05)', width: 1 },
          ticks: { stroke: 'rgba(255,255,255,0.08)', width: 1, size: 4 },
          font: '11px DejaVu Sans Mono, Menlo, Consolas, monospace',
          gap: 8,
          size: (self, values) => {
            if (!values) return 70
            const maxLen = Math.max(...values.map(v => (v ?? '').toString().length))
            return Math.max(70, maxLen * 8 + 16)
          },
        },
      ],
      cursor: {
        drag: { x: true, y: false },
        points: { show: true, size: 6, fill: '#faff69', stroke: '#faff69' },
      },
      scales: { x: { time: true } },
    }

    if (plotRef.current) plotRef.current.destroy()
    plotRef.current = new uPlot(opts, plotData as uPlot.AlignedData, chartRef.current)

    // Resize handler
    const ro = new ResizeObserver(() => {
      if (plotRef.current && containerRef.current) {
        const w = containerRef.current.clientWidth - 2
        if (w > 0) plotRef.current.setSize({ width: w, height: 300 })
      }
    })
    if (containerRef.current) ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      if (plotRef.current) {
        plotRef.current.destroy()
        plotRef.current = null
      }
      setTooltip(null)
    }
  }, [data])

  // Hide tooltip when mouse leaves
  const handleMouseLeave = () => setTooltip(null)

  const legendItems = data.map((s, idx) => ({
    label: buildLabel(s.metric),
    color: COLORS[idx % COLORS.length],
    lastVal: s.values.length > 0 ? s.values[s.values.length - 1][1] : 'N/A',
  }))

  return (
    <div ref={containerRef} className="graph-container" onMouseLeave={handleMouseLeave}>
      <div ref={chartRef} />

      {/* Custom tooltip */}
      {tooltip && tooltip.closest && (
        <div className="graph-tooltip" style={{
          position: 'fixed',
          left: Math.min(tooltip.x + 12, window.innerWidth - 320),
          top: tooltip.y,
          zIndex: 1000,
        }}>
          <div className="graph-tooltip-time">{tooltip.timestamp}</div>
          <div className="graph-tooltip-series">
            <span className="graph-tooltip-swatch" style={{ background: tooltip.closest.color }} />
            <span className="graph-tooltip-name">
              {tooltip.closest.metric.__name__ || ''}
            </span>
            <span className="graph-tooltip-val">{tooltip.closest.value}</span>
          </div>
          <div className="graph-tooltip-labels">
            {Object.entries(tooltip.closest.metric)
              .filter(([k]) => k !== '__name__')
              .map(([k, v]) => (
                <div key={k} className="graph-tooltip-label">
                  <span className="graph-tooltip-label-key">{k}</span>: {v}
                </div>
              ))}
          </div>
        </div>
      )}

      {legendItems.length > 0 && (
        <div className="graph-legend">
          {legendItems.map((item, i) => (
            <div key={i} className="graph-legend-item">
              <span className="graph-legend-swatch" style={{ background: item.color }} />
              <span className="graph-legend-label">{item.label}</span>
              <span className="graph-legend-value">{item.lastVal}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
