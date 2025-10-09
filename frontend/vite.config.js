import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  root: '.',
  build: {
    outDir: 'dist',
    emptyOutDir: true
  },
  server: {
    port: 5173,
    proxy: {
      '/auth': 'http://localhost:3001',
      '/search': 'http://localhost:3001',
      '/singer': 'http://localhost:3001',
      '/opera': 'http://localhost:3001',
      '/book': 'http://localhost:3001',
      '/subscription': 'http://localhost:3001',
      '/health': 'http://localhost:3001',
      '/views': 'http://localhost:3001'
    }
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src')
    }
  }
});
