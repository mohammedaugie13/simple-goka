package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	storage "github.com/lovoo/goka/storage/redis"
	"gopkg.in/redis.v5"
	"log"
	"net/http"
	"simple_goka/codec"
	"simple_goka/consumer"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	go func() {
		err := consumer.ConsumeBalance([]string{"127.0.0.1:9092"}, "test", "test")
		if err != nil {
			log.Printf("Error %v", err)
		}
	}()

	go func() {
		err := consumer.ConsumeThreshold([]string{"127.0.0.1:9092"}, "test2", "test", rdb)
		if err != nil {
			log.Printf("Error %v", err)
		}
	}()

	runView([]string{"127.0.0.1:9092"}, "test", rdb)

}

func runView(brokers []string, group string, rdb *redis.Client) {
	log.Println("RUN")
	g := goka.Group(group)
	view, err := goka.NewView(brokers,
		goka.GroupTable(g),
		new(codec.Codec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get("augi")
		var abv bool
		s, _ := storage.New(rdb, "producer")
		res, _ := s.Get("augi")
		json.Unmarshal(res, abv)
		log.Printf("View 2 %v", abv)

		data, _ := json.Marshal(value)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:1234/")
	go http.ListenAndServe(":9095", root)

	view.Run(context.Background())
}
