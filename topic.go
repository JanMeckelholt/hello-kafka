package main

import (
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
)

func ensureTopic(topic string, numPartitions int) {
	conn, err := kafka.Dial("tcp", KafkaHost)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err =
		kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic: topic,
			NumPartitions: numPartitions,
			ReplicationFactor: 1,
		},
	}
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	log.Printf(
		"Topic '%s' with %d partitions successfully created\n",
		topic,
		numPartitions,
	)
}