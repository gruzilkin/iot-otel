// Package web renders server-side HTML and serves static assets.
package web

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

var templates = template.Must(template.ParseFS(templateFS, "templates/*.html"))

// ChartPage is the data for the chart page template.
type ChartPage struct {
	DeviceID int64
	Data     template.JS // marshaled map[sensor][]Point, injected as a JS object
}

func RenderChart(w http.ResponseWriter, page ChartPage) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return templates.ExecuteTemplate(w, "chart.html", page)
}

// StaticHandler serves embedded static assets (e.g. /css/site.css).
func StaticHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
