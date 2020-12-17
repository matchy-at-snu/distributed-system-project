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
		e.Logger.Print("I got the input!")

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
					mapping[word] +=1
				} else {
					mapping[word] = 1
				}
			}
		}

		for k, v := range mapping {
			e.Logger.Print("I got the output! Check first result: ", k, ": ", v)
			break
		}

		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		encErr := encoder.Encode(mapping)
		if encErr != nil {
			return c.String(http.StatusBadRequest, encErr.Error())
		}

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}
