import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The frontend is built on the laptop (`make frontend`), not in the app container:
// Apps runs `npm install` for any package.json at the source root, and the
// workspace npm proxy served corrupt tarballs and 404s (2026-09-23), failing the
// deploy. dist/ is gitignored and shipped by the bundle's sync.include instead.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: { outDir: "dist", emptyOutDir: true, target: "es2020" },
  server: { proxy: { "/api": "http://localhost:8765" } },
});
