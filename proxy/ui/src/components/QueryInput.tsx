import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { apiFetch } from '../api/client'

const PROMQL_FUNCTIONS = [
  'abs','absent','absent_over_time','avg_over_time','ceil','changes','clamp','clamp_max','clamp_min',
  'count_over_time','day_of_month','day_of_week','day_of_year','days_in_month','delta','deriv',
  'double_exponential_smoothing','exp','floor','histogram_quantile','hour','idelta','increase',
  'irate','label_join','label_replace','last_over_time','ln','log2','log10','mad_over_time',
  'max_over_time','min_over_time','minute','month','predict_linear','present_over_time',
  'quantile_over_time','rate','resets','round','scalar','sgn','sort','sort_by_label',
  'sort_by_label_desc','sort_desc','sqrt','stddev_over_time','stdvar_over_time','sum_over_time',
  'time','timestamp','vector','year',
]
const PROMQL_AGGREGATIONS = [
  'avg','bottomk','count','count_values','group','limitk','limit_ratio','max','min',
  'quantile','stddev','stdvar','sum','topk',
]
const ALL_FUNCTIONS = [...PROMQL_FUNCTIONS, ...PROMQL_AGGREGATIONS].sort()
const FUNC_SET = new Set([...PROMQL_FUNCTIONS, ...PROMQL_AGGREGATIONS])

interface Suggestion { text: string; kind: 'metric' | 'function' | 'label' | 'value' }

interface Props {
  value: string
  onChange: (v: string) => void
  onExecute: () => void
}

const C_FUNC = '#228be6', C_METRIC = '#c1c2c5', C_LABEL = '#40c057'
const C_VALUE = '#ae3ec9', C_NUMBER = '#f76707', C_OP = '#868e96', C_TEXT = '#c1c2c5'

function highlightPromQL(text: string, metricSet: Set<string>): JSX.Element[] {
  const tokens: { text: string; color: string }[] = []
  const re = /("(?:[^"\\]|\\.)*")|(\b\d+\.?\d*(?:[eE][+-]?\d+)?[smhdw]?\b)|((?:!=|!~|=~|[=<>!]=?|[+\-*/^%]|and|or|unless|on|ignoring|group_left|group_right|by|without|bool|offset)\b)|([a-zA-Z_:][a-zA-Z0-9_:]*)|([\[\]{}(),])|(\s+)|(.)/g
  let m
  while ((m = re.exec(text)) !== null) {
    const [, str, num, op, ident, punct, ws, other] = m
    if (str) tokens.push({ text: str, color: C_VALUE })
    else if (num) tokens.push({ text: num, color: C_NUMBER })
    else if (op) tokens.push({ text: op, color: C_OP })
    else if (ident) {
      if (FUNC_SET.has(ident)) tokens.push({ text: ident, color: C_FUNC })
      else if (['by','without','on','ignoring','group_left','group_right','bool','offset','and','or','unless'].includes(ident)) tokens.push({ text: ident, color: C_OP })
      else if (metricSet.has(ident)) tokens.push({ text: ident, color: C_METRIC })
      else tokens.push({ text: ident, color: C_LABEL })
    }
    else if (punct) tokens.push({ text: punct, color: C_OP })
    else if (ws) tokens.push({ text: ws, color: C_TEXT })
    else if (other) tokens.push({ text: other, color: C_TEXT })
  }
  return tokens.map((t, i) => <span key={i} style={{ color: t.color }}>{t.text}</span>)
}

export default function QueryInput({ value, onChange, onExecute }: Props) {
  const [metrics, setMetrics] = useState<string[]>([])
  const [labels, setLabels] = useState<string[]>([])
  const [labelValues, setLabelValues] = useState<Record<string, string[]>>({})
  const [suggestions, setSuggestions] = useState<Suggestion[]>([])
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const highlightRef = useRef<HTMLDivElement>(null)
  const metricSet = useMemo(() => new Set(metrics), [metrics])

  const syncScroll = () => {
    if (textareaRef.current && highlightRef.current) {
      highlightRef.current.scrollTop = textareaRef.current.scrollTop
      highlightRef.current.scrollLeft = textareaRef.current.scrollLeft
    }
  }

  useEffect(() => {
    if (showSuggestions && dropdownRef.current) {
      const el = dropdownRef.current.children[selectedIdx] as HTMLElement
      if (el) el.scrollIntoView({ block: 'nearest' })
    }
  }, [selectedIdx, showSuggestions])

  useEffect(() => {
    apiFetch('/api/v1/label/__name__/values').then((r: any) => setMetrics(r.data || [])).catch(() => {})
    apiFetch('/api/v1/labels').then((r: any) => setLabels((r.data || []).filter((l: string) => l !== '__name__'))).catch(() => {})
  }, [])

  const fetchLabelValues = useCallback(async (label: string) => {
    if (labelValues[label]) return labelValues[label]
    try {
      const r = await apiFetch(`/api/v1/label/${label}/values`) as any
      const vals = r.data || []
      setLabelValues(prev => ({ ...prev, [label]: vals }))
      return vals
    } catch { return [] }
  }, [labelValues])

  const getContext = (text: string, cursorPos: number) => {
    const before = text.slice(0, cursorPos)
    // After = != =~ !~ followed by " inside {} → label value mode
    const valueMatch = before.match(/\{[^}]*?(\w+)\s*(?:!?=~?)\s*"([^"]*)$/)
    if (valueMatch) return { mode: 'value' as const, word: valueMatch[2], labelName: valueMatch[1] }
    // Inside {} — typing a label name
    if (before.lastIndexOf('{') > before.lastIndexOf('}')) {
      const labelMatch = before.match(/(?:[{,]\s*)([a-zA-Z_][\w]*)$/)
      if (labelMatch) return { mode: 'label' as const, word: labelMatch[1] }
      // Right after { or , or after closing " of previous matcher
      if (/[{,]\s*$/.test(before) || /"\s*,\s*$/.test(before)) return { mode: 'label' as const, word: '' }
      // Right after { with no chars
      if (/\{\s*$/.test(before)) return { mode: 'label' as const, word: '' }
    }
    // Function or metric name
    const wordMatch = before.match(/([a-zA-Z_:][a-zA-Z0-9_:]*)$/)
    if (wordMatch && wordMatch[1].length >= 1)
      return { mode: 'function' as const, word: wordMatch[1] }
    return { mode: 'function' as const, word: '' }
  }

  const updateSuggestions = useCallback(async (text: string, cursorOverride?: number) => {
    const cursorPos = cursorOverride ?? textareaRef.current?.selectionStart ?? text.length
    const ctx = getContext(text, cursorPos)
    if (!ctx.word && ctx.mode !== 'label' && ctx.mode !== 'value') {
      setSuggestions([]); setShowSuggestions(false); return
    }
    const lower = ctx.word.toLowerCase()
    let items: Suggestion[] = []
    switch (ctx.mode) {
      case 'value':
        if (ctx.labelName) {
          const vals = await fetchLabelValues(ctx.labelName)
          items = vals.filter((v: string) => v.toLowerCase().includes(lower)).slice(0, 20)
            .map((v: string) => ({ text: v, kind: 'value' as const }))
        }
        break
      case 'label':
        items = labels.filter(l => l.toLowerCase().includes(lower)).slice(0, 15)
          .map(l => ({ text: l, kind: 'label' as const }))
        break
      default:
        items = [
          ...ALL_FUNCTIONS.filter(f => f.toLowerCase().includes(lower)).slice(0, 8).map(f => ({ text: f, kind: 'function' as const })),
          ...metrics.filter(m => m.toLowerCase().includes(lower)).slice(0, 12).map(m => ({ text: m, kind: 'metric' as const })),
        ]
    }
    setSuggestions(items); setSelectedIdx(0); setShowSuggestions(items.length > 0)
  }, [metrics, labels, fetchLabelValues])

  const applySuggestion = (s: Suggestion) => {
    const cursorPos = textareaRef.current?.selectionStart ?? value.length
    const ctx = getContext(value, cursorPos)
    const before = value.slice(0, cursorPos), after = value.slice(cursorPos)
    let newValue = value, newCursor = cursorPos
    if (ctx.mode === 'value') {
      const match = before.match(/(.*=~?\s*")([^"]*)$/)
      if (match) { newValue = match[1] + s.text + '"' + after; newCursor = match[1].length + s.text.length + 1 }
    } else if (ctx.mode === 'label') {
      // Replace partial label text, or insert at cursor if no partial typed
      const match = before.match(/(.*?)([a-zA-Z_][\w]*)$/)
      if (match && match[2].length > 0) {
        newValue = match[1] + s.text + '="' + after; newCursor = match[1].length + s.text.length + 2
      } else {
        // Empty word — just insert label name at cursor
        newValue = before + s.text + '="' + after; newCursor = before.length + s.text.length + 2
      }
    } else {
      const match = before.match(/(.*?)([a-zA-Z_:][a-zA-Z0-9_:]*)$/)
      if (match) { const sfx = s.kind === 'function' ? '(' : ''; newValue = match[1] + s.text + sfx + after; newCursor = match[1].length + s.text.length + sfx.length }
    }
    onChange(newValue)
    if (ctx.mode !== 'label') setShowSuggestions(false)
    setTimeout(() => {
      textareaRef.current?.focus()
      textareaRef.current?.setSelectionRange(newCursor, newCursor)
      if (ctx.mode === 'label') updateSuggestions(newValue, newCursor)
    }, 10)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setSelectedIdx(i => Math.min(i + 1, suggestions.length - 1)); return }
      if (e.key === 'ArrowUp') { e.preventDefault(); setSelectedIdx(i => Math.max(i - 1, 0)); return }
      if (e.key === 'Tab' || (e.key === 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey)) {
        e.preventDefault(); applySuggestion(suggestions[selectedIdx]); return
      }
      if (e.key === 'Escape') { setShowSuggestions(false); return }
    }
    if (e.key === 'Enter') {
      e.preventDefault(); setShowSuggestions(false); onExecute()
    }
  }

  const KIND_COLORS: Record<string, string> = { function: '#228be6', metric: '#c1c2c5', label: '#40c057', value: '#ae3ec9' }
  const KIND_LABELS: Record<string, string> = { function: 'fn', metric: 'metric', label: 'label', value: 'val' }

  return (
    <div className="query-input-wrapper" style={{ position: 'relative', flex: 1 }}>
      <div className="query-editor">
        <div ref={highlightRef} className="query-highlight" aria-hidden="true">
          {highlightPromQL(value, metricSet)}<span> </span>
        </div>
        <textarea
          ref={textareaRef}
          className="query-textarea"
          value={value}
          onChange={e => { onChange(e.target.value); updateSuggestions(e.target.value) }}
          onKeyDown={handleKeyDown}
          onScroll={syncScroll}
          onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
          placeholder="PromQL query (Enter to execute)"
          rows={1}
          autoComplete="off"
          spellCheck={false}
        />
      </div>
      {showSuggestions && (
        <div className="suggestions-dropdown" ref={dropdownRef}>
          {suggestions.map((s, i) => (
            <div key={s.text + s.kind} className={`suggestion-item ${i === selectedIdx ? 'selected' : ''}`} onMouseDown={() => applySuggestion(s)}>
              <span style={{ fontSize: 9, fontWeight: 600, padding: '1px 4px', borderRadius: 2, marginRight: 6, flexShrink: 0, background: (KIND_COLORS[s.kind] || '#666') + '18', color: KIND_COLORS[s.kind] || '#666', textTransform: 'uppercase', letterSpacing: '0.3px', fontFamily: 'var(--font)' }}>
                {KIND_LABELS[s.kind] || s.kind}
              </span>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: KIND_COLORS[s.kind] || '#c1c2c5' }}>{s.text}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
