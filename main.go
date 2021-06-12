package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

const (
	KafkaHost = "localhost:29092"
	Topic = "hello-kafka"
)
func main() {
	ensureTopic(Topic, 1)
	writeMessage(Topic, "Hello Kafka!")
	readMessage(Topic)
}

func writeMessage(topic, msg string) {
	log.Printf("Send message '%s' to topic '%s'\n", msg, topic)
	w := &kafka.Writer{
		Addr: kafka.TCP(KafkaHost),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(msg),
	}); err != nil {
		log.Printf("Failed to write message %s: %v\n", msg, err)
		return
	}
	log.Printf("Successfully send message '%s' to topic '%s'\n", msg, topic)
}

func readMessage(topic string) {
	log.Printf("Start reading message from topic '%s'", topic)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KafkaHost},
		Topic: topic,
	})
	defer r.Close()
	msg, err := r.ReadMessage(context.Background())
	if err != nil {
		log.Printf("Failed to read message: %v\n", err)
		return
	}
	log.Printf("Received message from topic '%s' at partition %v, offset	%v: %s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
}