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

// NewMessage ...
func NewMessage(msg *pubsub.Message) Message {
	return Message{
		msg.ID,
		string(msg.Data),
		msg.PublishTime,
		msg.Attributes,
		msg.OrderingKey,
		msg.DeliveryAttempt,
	}
}

// JSON ...
func (m Message) JSON() string {
	obj, err := json.Marshal(m)
	if err != nil {
		log.Panicf("json.Marshal: %v", err)
	}
	return string(obj)
}

// PublishMessage ...
func PublishMessage(ctx context.Context, topic *pubsub.Topic, body []byte) {
	m := pubsub.Message{
		Data: body,
	}
	publishResult := topic.Publish(ctx, &m)
	id, err := publishResult.Get(ctx)
	if err != nil {
		log.Fatalln("publish error", err)
	} else {
		log.Println("published", id)
	}
}

var inflight = new(sync.Map)

func handle(ctx context.Context, msg *pubsub.Message) {
	inflight.Store(msg.ID, msg)
	m := NewMessage(msg)

	log.Println(m.JSON())
	fmt.Println(m.JSON())
}

func main() {
	ctx := context.Background()

	projectName := os.Args[1]
	subscriptionName := os.Args[2]
	mode := os.Args[3]

	var opts []option.ClientOption
	if path, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); ok {
		opts = append(opts, option.WithCredentialsFile(path))
	} else {
		credentials, err := google.FindDefaultCredentials(ctx)
		if err != nil {
			log.Panicf("google.FindDefaultCredentials: %v", err)
		}

		opts = append(opts, option.WithCredentials(credentials))
	}

	client, err := pubsub.NewClient(ctx, projectName, opts...)
	if err != nil {
		log.Panicf("pubsub.NewClient: %v", err)
	}

	switch mode {
	case "push":
		topic := client.Topic(subscriptionName)
		var body []byte
		if len(os.Args) > 4 {
			body = []byte(os.Args[4])
		} else {
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			body = scanner.Bytes()
		}
		PublishMessage(ctx, topic, body)
	case "publish":
		topic := client.Topic(subscriptionName)

		for {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				body := scanner.Bytes()
				PublishMessage(ctx, topic, body)
			}
		}
	case "subscribe":
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

		sub := client.Subscription(subscriptionName)
		log.Println("subscribed to", subscriptionName)
		sub.Receive(ctx, handle)
	case "drain":
		sub := client.Subscription(subscriptionName)
		sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			m := NewMessage(msg)
			log.Println(m.JSON())
			fmt.Println(m.JSON())
			msg.Ack()
		})
	}

}
