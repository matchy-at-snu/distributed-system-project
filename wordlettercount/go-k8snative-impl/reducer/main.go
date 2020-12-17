package main

import (
	"bytes"
	"encoding/gob"
	"github.com/labstack/echo"
	"net/http"
)

func main() {
	e := echo.New()

	e.GET("/reduce", func(c echo.Context) error {
		e.Logger.Info("I got the input!")

		body := c.QueryParam("body")
		buf := bytes.NewBuffer([]byte(body))

		var reduceData = map[string][]int{}

		decoder := gob.NewDecoder(buf)
		_ = decoder.Decode(&reduceData)

		var reducing = map[string]int{}

		for key, value := range reduceData {
			reducing[key] = 0
			for _, count := range value {
				reducing[key] += count
			}
		}

		for k, v := range reducing {
			e.Logger.Info("I got the output! Check first result: ", k, ": ", v)
			break;
		}

		buf = new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		_ = encoder.Encode(reducing)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}
