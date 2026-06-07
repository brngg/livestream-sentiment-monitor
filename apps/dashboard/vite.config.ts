import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      "/state": "http://127.0.0.1:8090",
      "/events": "http://127.0.0.1:8090",
      "/sessions": "http://127.0.0.1:8090",
      "/labels": "http://127.0.0.1:8090",
      "/signal-window-labels": "http://127.0.0.1:8090",
      "/transcript": {
        target: "http://127.0.0.1:8090",
        ws: true
      },
      "/health": "http://127.0.0.1:8090"
    }
  },
  test: {
    environment: "jsdom",
    globals: false,
    setupFiles: "./src/setupTests.ts"
  }
});
