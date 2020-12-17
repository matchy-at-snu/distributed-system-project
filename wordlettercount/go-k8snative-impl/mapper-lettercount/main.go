package main

import (
	"encoding/json"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
)

func main() {
	e := echo.New()
	e.Use(middleware.BodyLimit("10MB"))

	e.POST("/map", func(c echo.Context) error {
		var (
			b   []byte
			err error
		)
		if b, err = ioutil.ReadAll(c.Request().Body); err != nil {
			e.Logger.Fatal(err)
		}

		str := string(b)

		lines := strings.Split(str, "\n")

		mapping := map[string]int{}

		// FIXME: regex not working
		var reNoChar = regexp.MustCompile("[^\\p{Greek}\\w]")
		var reEmDash = regexp.MustCompile("[0-9]|_")

		for _, line := range lines {
			s := reEmDash.ReplaceAllString(
				reNoChar.ReplaceAllString(line, ""), "")
			chars := strings.Split(s, "")
			for _, c := range chars {
				if c != "" {
					mapping[strings.ToLower(c)] += 1
				}
			}
		}

		data, encErr := json.Marshal(mapping)
		if encErr != nil {
			e.Logger.Fatal(encErr)
		}

		return c.JSONBlob(http.StatusOK, data)
	})

	e.Logger.Fatal(e.Start(":8080"))
}
