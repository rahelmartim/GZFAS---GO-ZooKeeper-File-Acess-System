// Package web holds the dashboard page. The HTTP server embeds it.
package web

import "embed"

// Files are the page, the styles, and the scripts.
//
//go:embed index.html style.css app.js
var Files embed.FS
