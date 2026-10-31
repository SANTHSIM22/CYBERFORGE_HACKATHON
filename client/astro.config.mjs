// @ts-check
import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';

// Static build served by nginx (see ../deploy/nginx/default.conf). The pages
// contain no tenant data: every number is fetched at runtime from /api with an
// identity token, and the Go API authorizes each request (docs/architecture.md).
export default defineConfig({
  output: 'static',
  build: { inlineStylesheets: 'auto' },
  vite: {
    plugins: [tailwindcss()],
    server: {
      // `npm run dev`: same /api and /demo paths as production, straight to the Compose services.
      proxy: {
        '/api-direct': { target: 'http://localhost:8090', changeOrigin: true, rewrite: (p) => p.replace(/^\/api-direct/, '') },
        '/api': { target: 'http://localhost:8080', changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, '') },
        '/demo': { target: 'http://localhost:8081', changeOrigin: true, rewrite: (p) => p.replace(/^\/demo/, '') },
      },
    },
  },
});
