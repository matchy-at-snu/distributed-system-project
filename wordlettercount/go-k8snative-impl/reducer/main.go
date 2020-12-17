package main

import (
	"encoding/json"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"io/ioutil"
	"net/http"
)

func main() {
	e := echo.New()
	e.Use(middleware.BodyLimit("10MB"))

	e.POST("/reduce", func(c echo.Context) error {

		body, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			e.Logger.Fatal(err)
		}

		var reduceData = map[string][]int{}

		decError := json.Unmarshal(body, &reduceData)
		if decError != nil {
			e.Logger.Fatal("decode error:", decError)
		}

		var reducing = map[string]int{}

		for key, value := range reduceData {
			reducing[key] = 0
			for _, count := range value {
				reducing[key] += count
			}
		}

		data, encErr := json.Marshal(reducing)
		if encErr != nil {
			e.Logger.Fatal(encErr)
		}

		return c.JSONBlob(http.StatusOK, data)
	})

	e.Logger.Fatal(e.Start(":8080"))
}
