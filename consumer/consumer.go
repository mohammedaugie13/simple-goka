package consumer

import (
	"context"
	"encoding/json"
	"github.com/lovoo/goka"
	storage "github.com/lovoo/goka/storage/redis"
	"gopkg.in/redis.v5"
	"log"
	"simple_goka/codec"
	"simple_goka/event"
	"time"
)

func ConsumeBalance(brokers []string, group string, stream string) error {
	cdc := new(codec.Codec)
	log.Println("Start Consume Balance")

	tmc := goka.NewTopicManagerConfig()
	tmc.Stream.Replication = 1
	tmc.Table.Replication = 1
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(stream, 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", stream, err)
	}
	input := goka.Input(goka.Stream(stream), cdc, func(ctx goka.Context, msg interface{}) {
		evnt := msg
		walletId := evnt.(*event.Event).WalletID
		amount := evnt.(*event.Event).Amount
		prevValue := ctx.Value()
		var Data event.Event
		if prevValue == nil {
			log.Println("Previous Data is Empty")
			Data.WalletID = walletId
			Data.Amount = amount
			Data.Timestamp = time.Now().UnixNano()
		} else {

			prevData := prevValue.(*event.Event)
			log.Printf("Previous Data Balance is Amount %v", prevData)
			Data.WalletID = walletId
			Data.Amount = prevData.Amount + amount
			Data.Timestamp = time.Now().UnixNano()

		}
		ctx.SetValue(Data)
	})
	graph := goka.DefineGroup(goka.Group(group), input, goka.Persist(cdc))

	opts := []goka.ProcessorOption{}
	opts = append(opts, goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)))

	processor, err := goka.NewProcessor(brokers, graph, opts...)
	if err != nil {
		log.Printf("hehe %v", err)
		return err
	}
	return processor.Run(context.Background())
}

func ConsumeThreshold(brokers []string, group string, stream string, rdb *redis.Client) error {
	cdc := new(codec.Codec)
	log.Println("Start Consume Threshold")

	tmc := goka.NewTopicManagerConfig()
	tmc.Stream.Replication = 1
	tmc.Table.Replication = 1
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(stream, 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", stream, err)
	}
	input := goka.Input(goka.Stream(stream), cdc, func(ctx goka.Context, msg interface{}) {
		evnt := msg
		walletId := evnt.(*event.Event).WalletID
		amount := evnt.(*event.Event).Amount
		timestamp := evnt.(*event.Event).Timestamp
		prevValue := ctx.Value()
		var Data event.Event
		var aboveThreshold bool
		if prevValue == nil {
			Data.Amount = amount
			Data.Timestamp = timestamp
			Data.WalletID = walletId
			aboveThreshold = false
		} else {
			prevData := prevValue.(*event.Event)
			log.Printf("Previous Data Threshold %v", prevData)
			if timestamp-prevData.Timestamp < 120000000000 {
				log.Printf("Below two minutes, timediff: %v", timestamp-prevData.Timestamp)
				Data.Amount = prevData.Amount + amount
				Data.Timestamp = timestamp
				Data.WalletID = walletId
				log.Printf("Previous Data Amount Window %v", prevData.Amount+amount)

				if prevData.Amount+amount > 10000 {
					log.Println("Set true")
					aboveThreshold = true
				} else {
					aboveThreshold = false

				}

			} else {
				Data.Amount = amount
				Data.Timestamp = timestamp
				Data.WalletID = walletId
			}

		}
		log.Printf("Th %v", aboveThreshold)

		s, _ := storage.New(rdb, "producer")
		dumped, _ := json.Marshal(aboveThreshold)
		s.Set(walletId, dumped)
		ctx.SetValue(Data)
	})
	graph := goka.DefineGroup(goka.Group(group), input, goka.Persist(cdc))

	opts := []goka.ProcessorOption{}

	opts = append(opts, goka.WithStorageBuilder(storage.RedisBuilder(rdb, "producer")))
	opts = append(opts, goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)))

	processor, err := goka.NewProcessor(brokers, graph, opts...)
	if err != nil {
		log.Printf("hehe %v", err)
		return err
	}
	return processor.Run(context.Background())
}
