package main

import (
	"encoding/json"
	"github.com/labstack/echo"
	"net/http"
	"strings"
)

func main() {
	e := echo.New()

	e.GET("/map", func(c echo.Context) error {
		str := c.QueryParam("str")

		e.Logger.Print(str)

		lines := strings.Split(str, "\n")

		mapping := map[string]int{}

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

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
		c.Response().WriteHeader(http.StatusOK)
		return json.NewEncoder(c.Response()).Encode(mapping)
	})

	e.Logger.Fatal(e.Start(":8080"))
}
