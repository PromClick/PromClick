import { useState, useCallback } from 'react'
import { apiFetch, apiPost } from '../api/client'

interface QueryResult {
  data: unknown
  loading: boolean
  error: string | null
}

export function useQuery() {
  const [result, setResult] = useState<QueryResult>({ data: null, loading: false, error: null })

  const execute = useCallback(async (query: string, time?: string) => {
    setResult({ data: null, loading: true, error: null })
    try {
      const params: Record<string, string> = { query }
      if (time) params.time = time
      const resp = await apiPost('/api/v1/query', params)
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

export function useMetricNames() {
  const [names, setNames] = useState<string[]>([])

  const fetch = useCallback(async () => {
    try {
      const resp = await apiFetch('/api/v1/label/__name__/values') as { data?: string[] }
      if (resp.data) setNames(resp.data)
    } catch {
      // ignore
    }
  }, [])

  return { names, fetch }
}
