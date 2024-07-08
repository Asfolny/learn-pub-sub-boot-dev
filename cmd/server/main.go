package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	con := "amqp://guest:guest@localhost:5672/"
	serv, _ := amqp.Dial(con)
	defer serv.Close()

	fmt.Println("Connected!")

	subChan, _ := serv.Channel()
	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey

	gamelogic.PrintServerHelp()

	err := pubsub.SubscribeGob(
		serv,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		true,
		handlerLogs(),
	)
	if err != nil {
		log.Fatalf("could not starting consuming logs: %v", err)
	}

sysLoop:
	for {
		input := gamelogic.GetInput()

		if len(input) < 1 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Pausing...")
			playState := routing.PlayingState{IsPaused: true}
			pubsub.PublishJSON(subChan, exchange, key, playState)

		case "resume":
			fmt.Println("Resuming...")
			playState := routing.PlayingState{IsPaused: false}
			pubsub.PublishJSON(subChan, exchange, key, playState)

		case "quit":
			break sysLoop

		default:
			fmt.Println("I don't know that command")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down...")
}

func handlerLogs() func(gamelog routing.GameLog) pubsub.Acktype {
	return func(gamelog routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
