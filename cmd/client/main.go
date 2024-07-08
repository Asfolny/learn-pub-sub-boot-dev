package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")

	con := "amqp://guest:guest@localhost:5672/"
	serv, _ := amqp.Dial(con)
	defer serv.Close()

	name, _ := gamelogic.ClientWelcome()
	pubsub.DeclareAndBind(
		serv,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+name,
		routing.PauseKey,
		false,
	)

	state := gamelogic.NewGameState(name)

	err := pubsub.SubscribeJSON(
		serv,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+name,
		routing.PauseKey,
		false,
		handlerPause(state),
	)
	if err != nil {
		log.Fatalln(err)
	}

	subChan, _ := serv.Channel()
	err = pubsub.SubscribeJSON(
		serv,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+name,
		routing.ArmyMovesPrefix+".*",
		false,
		handlerMove(state, subChan),
	)
	if err != nil {
		log.Fatalln(err)
	}

	pubsub.SubscribeJSON(
		serv,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		true,
		handlerWar(state, subChan),
	)

clientLoop:
	for {
		input := gamelogic.GetInput()

		if len(input) < 1 {
			continue
		}

		switch input[0] {
		case "spawn":
			state.CommandSpawn(input)

		case "move":
			armyMov, _ := state.CommandMove(input)
			pubsub.PublishJSON(
				subChan,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+name,
				armyMov,
			)

		case "status":
			state.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(input) < 2 {
				gamelogic.PrintClientHelp()
				continue
			}

			spamCount, err := strconv.Atoi(input[1])
			if err != nil {
				gamelogic.PrintClientHelp()
				continue
			}

			for i := 0; i < spamCount; i++ {
				logMsg := gamelogic.GetMaliciousLog()
				err := publishGameLog(
					subChan,
					name,
					logMsg)
				if err != nil {
					log.Fatal(err)
				}
			}

		case "quit":
			gamelogic.PrintQuit()
			break clientLoop

		default:
			fmt.Println("Not a valid command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(playState routing.PlayingState) pubsub.Acktype {
		gs.HandlePause(playState)
		return pubsub.Ack
	}
}

func handlerMove(
	gs *gamelogic.GameState,
	publishCh *amqp.Channel,
) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")

		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerWar(
	gs *gamelogic.GameState,
	publishCh *amqp.Channel,
) func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(dw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Println("error: unknown war outcome")
		return pubsub.NackDiscard
	}
}

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}
