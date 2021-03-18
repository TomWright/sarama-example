package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tomwright/grace"
	"github.com/tomwright/gracesarama"
	"log"
	"os"
	"strings"
	"time"
)

const (
	consumerGroup = "my-consumer-group"
	topic         = "my-topic"
)

func main() {
	g := grace.Init(context.Background())

	cfg := sarama.NewConfig()

	cfg.Version = sarama.V2_1_0_0

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true

	consumerGroupRunner := InitConsumerGroup(cfg, consumerGroup, map[string]ConsumerFn{
		topic: handler,
	})
	producerRunner := InitProducer(cfg)
	produceInput := producerRunner.Input()

	g.Run(consumerGroupRunner)
	g.Run(producerRunner)

	go func() {
		for {
			select {
			case <-g.Context().Done():
				return
			case <-time.After(time.Second * 20):
				for x := 0; x < 5; x++ {
					produceInput <- &sarama.ProducerMessage{
						Topic: topic,
						Value: sarama.StringEncoder(fmt.Sprintf("message-%d", x)),
					}
				}
			}
		}
	}()

	<-g.Context().Done()
	log.Println("Shutting down")

	g.Wait()
}

func handler(ctx context.Context, message *sarama.ConsumerMessage) error {
	log.Printf("handle message: %s", string(message.Value))
	return nil
}

func InitConsumerGroup(config *sarama.Config, consumerGroup string, handlers map[string]ConsumerFn) *gracesarama.ConsumerGroupRunner {
	if handlers == nil {
		handlers = make(map[string]ConsumerFn)
	}
	topics := make([]string, len(handlers))
	i := 0
	for topic := range handlers {
		topics[i] = topic
		i++
	}

	consumerGroupRunner := gracesarama.NewConsumerGroupRunner(
		strings.Split(os.Getenv("KAFKA_CONSUMER_ADDRESS"), ","), consumerGroup, config,
		topics, NewConsumerGroupHandler(handlers))

	consumerGroupRunner.ErrorHandlerFn = func(err error) {
		log.Printf("consumer group error: %s", err)
	}

	consumerGroupRunner.LogFn = func(format string, a ...interface{}) {
		log.Printf(format, a...)
	}

	return consumerGroupRunner
}

// ConsumerFn is a func that can be used to handle consumed messages.
// If an error is returned the message is not marked as consumed.
type ConsumerFn func(ctx context.Context, message *sarama.ConsumerMessage) error

// NewConsumerGroupHandler returns a consumer group handler than will send messages in the given topic
// to the given handler.
// handlers is topic => handler.
func NewConsumerGroupHandler(handlers map[string]ConsumerFn) *handlerMapConsumerGroupHandler {
	return &handlerMapConsumerGroupHandler{
		handlers: handlers,
	}
}

type handlerMapConsumerGroupHandler struct {
	handlers map[string]ConsumerFn
}

// Setup can be used to set things up when a consumer group session is established.
func (handlerMapConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup can be used to clean up once a consumer group session is released.
func (handlerMapConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim consumes a message and marks the message as consumed if no error is returned.
func (h handlerMapConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		handler, ok := h.handlers[msg.Topic]
		if !ok {
			return fmt.Errorf("missing handler for topic: %s", msg.Topic)
		}
		if err := handler(sess.Context(), msg); err != nil {
			return err
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func InitProducer(saramaCfg *sarama.Config) *gracesarama.ProducerRunner {
	producerRunner := gracesarama.NewProducerRunner(strings.Split(os.Getenv("KAFKA_PRODUCER_ADDRESS"), ","), saramaCfg)

	producerRunner.ErrorHandlerFn = func(err *sarama.ProducerError) {
		log.Printf("producer error [%s]: %s", err.Msg.Topic, err)
	}

	producerRunner.SuccessHandlerFn = func(msg *sarama.ProducerMessage) {
		log.Printf("producer success [%s]", msg.Topic)
	}

	return producerRunner
}
