package main

import (
	"encoding/json"
	"github.com/labstack/echo"
	"net/http"
)

func main() {
	e := echo.New()

	e.GET("/reduce", func(c echo.Context) error {
		e.Logger.Print("I got the input!")

		body := c.QueryParam("body")
		//buf := bytes.NewBuffer([]byte(body))

		var reduceData = map[string][]int{}

		//decoder := gob.NewDecoder(buf)
		//_ = decoder.Decode(&reduceData)
		decError := json.Unmarshal([]byte(body), &reduceData)
		if decError != nil {
			e.Logger.Fatal("decode error:", decError)
		}

		e.Logger.Print(reduceData)

		var reducing = map[string]int{}

		for key, value := range reduceData {
			reducing[key] = 0
			for _, count := range value {
				reducing[key] += count
			}
		}

		for k, v := range reducing {
			e.Logger.Print("I got the output! Check first result: ", k, ": ", v)
			break
		}

		//buf = new(bytes.Buffer)
		//encoder := gob.NewEncoder(buf)
		//_ = encoder.Encode(reducing)
		//
		//return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
		c.Response().WriteHeader(http.StatusOK)
		return json.NewEncoder(c.Response()).Encode(reducing)
	})

	e.Logger.Fatal(e.Start(":8080"))
}
