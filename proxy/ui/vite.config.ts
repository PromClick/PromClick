import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist', minify: true },
  server: {
    proxy: {
      '/api': 'http://localhost:9099',
      '/-': 'http://localhost:9099',
    }
  }
})
