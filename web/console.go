package web

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed static
var staticFiles embed.FS

// Register serves the embedded web console.
func Register(r *gin.Engine) {
	sub, _ := fs.Sub(staticFiles, "static")
	fileServer := http.StripPrefix("/console", http.FileServer(http.FS(sub)))
	r.GET("/console/*path", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		c.Header("Pragma", "no-cache")
		c.Header("Expires", "0")
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/console/")
	})
}
