# AzurePulse — Cloud Data Engineering & Analytics Platform

## Instant Deployment & Local Setup Guide

### Option A: Zero-Build Instant Deploy (Netlify Drop / GitHub Pages / Browser)
The `dist/index.html` file in this repository is already pre-compiled into a standalone production bundle:
- **Open directly in browser:** Double-click `dist/index.html`.
- **Netlify Drop / Cloudflare Pages / Azure Static Web Apps:** Drag and drop the `dist` folder directly into Netlify Drop (`https://app.netlify.com/drop`).

### Option B: Deploy to Vercel / Netlify via GitHub
All configuration files (`package.json`, `vite.config.ts` with `base: './'`, `vercel.json`, and `netlify.toml`) are at the repository root:
1. Extract this zip into your repository root (make sure `package.json` is in the root directory, not inside a nested subfolder).
2. Push to GitHub and import into **Vercel** or **Netlify** (Build Command: `npm run build`, Output Directory: `dist`).

### Option C: Local Vite Development Server
```bash
npm install
npm run dev
```
