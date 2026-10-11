import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The editor talks to the Bocagrande API (python -m bocagrande.api.app); in development it is proxied.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://127.0.0.1:8000" } },
});
