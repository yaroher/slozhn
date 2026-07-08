import { defineConfig } from "vite";

export default defineConfig({
  server: {
    fs: { allow: [".."] }, // access to ../core/pkg
  },
});
