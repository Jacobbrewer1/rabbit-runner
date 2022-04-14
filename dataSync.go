package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type (
	rabbit struct {
		User     *string   `json:"user,omitempty"`
		Password *string   `json:"password,omitempty"`
		Location *string   `json:"location,omitempty"`
		Queues   *[]string `json:"queues,omitempty"`
	}
)

func (r rabbit) IsPopulated() bool {
	return r.User != nil && r.Password != nil && r.Location != nil && r.Queues != nil
}

func init() {
	log.Println("initializing logging")
	//log.SetPrefix("LOG: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
	log.Println("logging initialized")
}

func main() {
	msg, err := ReadMessage()
	if err != nil {
		log.Println(err)
		fmt.Scanln()
		os.Exit(1)
	}

	rab, err := ReadConfig()
	if err != nil {
		log.Println(err)
		fmt.Scanln()
		os.Exit(1)
	}

	log.Println("connecting to rabbit")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:5672/", *rab.User, *rab.Password, *rab.Location))
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	log.Println("rabbit connected")

	log.Println("opening channel")
	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
		fmt.Scanln()
		os.Exit(1)
	}
	log.Println("channel opened")

	for _, q := range *rab.Queues {
		log.Printf("publishing message to queue %s\n", q)
		err = ch.Publish(
			"",
			q,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			})
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("message published to queue %s\n", q)
	}
}

func ReadConfig() (rabbit, error) {
	var rab rabbit
	if abs, exists := findFile("./config.json"); exists {
		log.Println("config detected - reading file")

		c, err := ioutil.ReadFile(abs)
		if err != nil {
			return rab, err
		}

		log.Println(string(c))
		err = json.Unmarshal(c, &rab)
		if err != nil {
			return rab, err
		} else if !rab.IsPopulated() {
			return rab, errors.New("rabbit is not populated correctly")
		}
		return rab, nil
	}
	return rab, errors.New("config does not exist")
}

func ReadMessage() (string, error) {
	if abs, exists := findFile("./message.json"); exists {
		log.Println("message detected - reading file")

		c, err := ioutil.ReadFile(abs)
		if err != nil {
			return "", err
		} else if len(c) == 0 {
			return "", errors.New("no message in message file")
		}
		return string(c), nil
	}
	return "", errors.New("message does not exist")
}

func findFile(path string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	log.Println(abs)

	file, err := os.Open(abs)
	if err != nil {
		return "", false
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	return abs, true
}
