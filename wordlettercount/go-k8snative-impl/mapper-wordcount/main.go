package main

import (
	"bytes"
	"encoding/gob"
	"github.com/labstack/echo"
	"net/http"
	"regexp"
	"strings"
)

func main() {
	e := echo.New()

	e.GET("/map", func(c echo.Context) error {
		e.Logger.Info("I got the input!")

		str := c.QueryParam("str")

		lines := strings.Split(str, "\n")

		wordCountMap := map[string]int{}

		var reNoChar = regexp.MustCompile("[^\\p{Greek}-]")
		var reEmDash = regexp.MustCompile("--+")

		for _, line := range lines {
			s := reEmDash.ReplaceAllString(
				reNoChar.ReplaceAllString(line, ""), "")
			words := strings.Split(s, " ")
			for _, word := range words {
				wordCountMap[word] += 1
			}
		}

		for k, v := range wordCountMap {
			e.Logger.Info("I got the output! Check first result: ", k, ": ", v)
			break;
		}


		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		_ = encoder.Encode(wordCountMap)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}
