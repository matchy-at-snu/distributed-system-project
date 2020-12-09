package main

import (
	"bytes"
	"encoding/gob"
	"github.com/labstack/echo"
	"net/http"
	"strings"
)

func main() {
	e := echo.New()

	e.GET("/map", func(c echo.Context) error {
		str := c.QueryParam("str")

		lines := strings.Split(str, "\n")

		wordCountMap := map[string]int{}

		for _, line := range lines {
			words := strings.Split(line, " ")
			for _, word := range words {
				wordCountMap[word] += 1
			}
		}

		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		_ = encoder.Encode(wordCountMap)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}