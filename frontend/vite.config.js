import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  root: '.',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks: {
          'vendor-react': ['react', 'react-dom'],
          'vendor-d3': ['d3'],
        }
      }
    }
  },
  server: {
    host: true,
    port: 5173,
    allowedHosts: [
      'endorsed-scenario-instantly-emissions.trycloudflare.com',
      '.trycloudflare.com'
    ],
    proxy: {
      '/auth': 'web-production-31a17.up.railway.app',
      '/search': 'web-production-31a17.up.railway.app',
      '/singer': 'web-production-31a17.up.railway.app',
      '/opera': 'web-production-31a17.up.railway.app',
      '/book': 'web-production-31a17.up.railway.app',
      '/subscription': 'web-production-31a17.up.railway.app',
      '/health': 'web-production-31a17.up.railway.app',
      '/views': 'web-production-31a17.up.railway.app'
    }
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src')
    }
  }
});
