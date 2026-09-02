import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // The UMD bundle is loaded directly by the Airflow UI, where Node's
  // `process` global does not exist. Inline the production branch so React
  // never dereferences it at runtime.
  define: { 'process.env.NODE_ENV': '"production"' },
  build: {
    lib: { entry: 'src/main.tsx', name: 'Cascade', formats: ['umd'], fileName: () => 'cascade.umd.cjs' },
    outDir: '../plugins/cascade/static',
    emptyOutDir: true,
    // Airflow renders this bundle's default export inside its own React tree
    // and publishes React on the window. Bundling a second copy would leave
    // every hook resolving against a null dispatcher.
    rollupOptions: {
      external: ['react', 'react-dom', 'react-dom/client', 'react/jsx-runtime'],
      output: {
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM',
          'react-dom/client': 'ReactDOM',
          'react/jsx-runtime': 'ReactJSXRuntime',
        },
      },
    },
  },
});
