package main

import (
	"encoding/json"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"net/http"
	"strings"
)

func main() {
	e := echo.New()
	e.Use(middleware.BodyLimit("10GB"))

	e.POST("/map", func(c echo.Context) error {
		var str string
		if bindErr := c.Bind(&str); bindErr != nil {
			e.Logger.Fatal(bindErr)
		}

		e.Logger.Print(str[:100]) // debug string, to be commented out!

		lines := strings.Split(str, "\n")

		mapping := map[string]int{}

		// FIXME: regex not working
		//var reNoChar = regexp.MustCompile("[^\\p{Greek}-]")
		//var reEmDash = regexp.MustCompile("--+")

		for _, line := range lines {
			//s := reEmDash.ReplaceAllString(
			//	reNoChar.ReplaceAllString(line, ""), "")
			words := strings.Split(line, " ")
			for _, word := range words {
				if _, prs := mapping[word]; prs {
					mapping[word] += 1
				} else {
					mapping[word] = 1
				}
			}
		}

		e.Logger.Print("I got the output! Check first 5 result: ")
		var count = 0
		for k, v := range mapping {
			e.Logger.Print(k, ": ", v)
			count++
			if count >= 5 {
				break
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
