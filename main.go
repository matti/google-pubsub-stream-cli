package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// Message ...
type Message struct {
	ID              string            `json:"id"`
	Body            string            `json:"body"`
	PublishTime     time.Time         `json:"publishTime"`
	Attributes      map[string]string `json:"attributes"`
	OrderingKey     string            `json:"orderingKey"`
	DeliveryAttempt *int              `json:"deliveryAttempt"`
}

var inflight = new(sync.Map)

func handle(ctx context.Context, msg *pubsub.Message) {
	inflight.Store(msg.ID, msg)
	m := Message{
		msg.ID,
		string(msg.Data),
		msg.PublishTime,
		msg.Attributes,
		msg.OrderingKey,
		msg.DeliveryAttempt,
	}

	obj, err := json.Marshal(m)
	if err != nil {
		log.Panicf("json.Marshal: %v", err)
	}
	log.Println(string(obj))
	fmt.Println(string(obj))
}

func main() {
	ctx := context.Background()

	credentials, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		log.Panicf("google.FindDefaultCredentials: %v", err)
	}

	client, err := pubsub.NewClient(ctx, "botway", option.WithCredentials(credentials))
	if err != nil {
		log.Panicf("pubsub.NewClient: %v", err)
	}

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			id := scanner.Text()
			m, loaded := inflight.LoadAndDelete(id)
			if loaded {
				m.(*pubsub.Message).Ack()
				log.Println("ack", "done", id)
			} else {
				log.Println("ack", "notfound", id)
			}

		}
	}()

	subscriptionName := os.Args[1]
	sub := client.Subscription(subscriptionName)
	sub.Receive(ctx, handle)
}
