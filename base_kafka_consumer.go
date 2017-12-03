// Copyright 2018 NTT Group

// Permission is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in the Software
// without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to the following
// conditions:

// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
// PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

package anki

import (
	"encoding/json"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

// CreateKafkaConsumer creates a Kafka consumer which is conected to the respective Kafka server
func CreateKafkaConsumer(zookeeperConn string, consumerGroup string, statusCh chan Status) (*consumergroup.ConsumerGroup, error) {
	plog.Println("Starting Consumer")
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	consumer, err := consumergroup.JoinConsumerGroup(consumerGroup, []string{"Status"}, []string{zookeeperConn}, config)
	if err != nil {
		plog.Println("Starting Consumer")
		plog.Println("ERROR: Failed to join consumer group", consumerGroup, err)
		return nil, err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)

	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			plog.Println("ERROR: Error closing the consumer", err)
		}

		plog.Println("INFO: Consumer closed")
		os.Exit(0)
	}()

	go func() {
		for err := range consumer.Errors() {
			plog.Println(err)
		}
	}()

	go func() {
		plog.Println("Waiting for messages")
		for message := range consumer.Messages() {
			plog.Printf("INFO: Topic: %s\t Partition: %v\t Offset: %v\n", message.Topic, message.Partition, message.Offset)

			e := handler(message, statusCh)
			if e != nil {
				plog.Fatal(e)
				consumer.Close()
			} else {
				consumer.CommitUpto(message)
			}
		}
	}()

	return consumer, nil
}

func handler(m *sarama.ConsumerMessage, statusCh chan Status) error {
	plog.Printf("INFO: Received message: %s", m.Value)
	update := Status{}
	err := json.Unmarshal(m.Value, &update)
	if err != nil {
		plog.Printf("WARNING: Could not unmarshal message, ignoring: %s", m.Value)
		return nil
	}
	statusCh <- update
	return nil
}
