import { useState, useCallback } from 'react'
import { apiPost } from '../api/client'

interface RangeResult {
  data: unknown
  loading: boolean
  error: string | null
}

export function useQueryRange() {
  const [result, setResult] = useState<RangeResult>({ data: null, loading: false, error: null })

  const execute = useCallback(async (query: string, start: string, end: string, step: string) => {
    setResult({ data: null, loading: true, error: null })
    try {
      const resp = await apiPost('/api/v1/query_range', { query, start, end, step })
      setResult({ data: resp, loading: false, error: null })
      return resp
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setResult({ data: null, loading: false, error: msg })
      return null
    }
  }, [])

  return { ...result, execute }
}
