package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type KafkaReader[T any] struct {
	consumer      sarama.Consumer
	partConsumer  sarama.PartitionConsumer
	groupConsumer sarama.ConsumerGroup

	topic     string
	partition int32
	offset    int64
	useGroup  bool
	groupID   string

	messagesChan chan *sarama.ConsumerMessage
	errorsChan   chan error

	deserialize func([]byte) (*T, error)
	closed      bool
}

type KafkaReaderConfig struct {
	Brokers          []string
	Topic            string
	Partition        int32
	Offset           int64
	GroupID          string
	UseConsumerGroup bool
	SaramaConfig     *sarama.Config
}

func NewKafkaReader[T any](config KafkaReaderConfig, deserialize func([]byte) (*T, error)) (*KafkaReader[T], error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if deserialize == nil {
		return nil, fmt.Errorf("deserialize function is required")
	}

	if config.Offset == 0 {
		config.Offset = sarama.OffsetNewest
	}

	saramaConfig := config.SaramaConfig
	if saramaConfig == nil {
		saramaConfig = sarama.NewConfig()
		saramaConfig.Consumer.Return.Errors = true
		saramaConfig.Version = sarama.V4_1_0_0
	}

	reader := &KafkaReader[T]{
		topic:        config.Topic,
		partition:    config.Partition,
		offset:       config.Offset,
		useGroup:     config.UseConsumerGroup,
		groupID:      config.GroupID,
		deserialize:  deserialize,
		messagesChan: make(chan *sarama.ConsumerMessage, 100),
		errorsChan:   make(chan error, 10),
	}

	if config.UseConsumerGroup {
		if config.GroupID == "" {
			return nil, fmt.Errorf("group ID is required when using consumer group")
		}

		group, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer group: %w", err)
		}
		reader.groupConsumer = group

		go reader.consumeGroup(context.Background())
	} else {
		consumer, err := sarama.NewConsumer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
		reader.consumer = consumer

		partConsumer, err := consumer.ConsumePartition(config.Topic, config.Partition, config.Offset)
		if err != nil {
			consumer.Close()
			return nil, fmt.Errorf("failed to create partition consumer: %w", err)
		}
		reader.partConsumer = partConsumer

		go reader.forwardMessages()
	}

	return reader, nil
}

func NewKafkaJSONReader[T any](config KafkaReaderConfig) (*KafkaReader[T], error) {
	deserialize := func(data []byte) (*T, error) {
		var item T
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
		return &item, nil
	}
	return NewKafkaReader[T](config, deserialize)
}

func (r *KafkaReader[T]) Read(ctx context.Context) (*T, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-r.errorsChan:
		return nil, fmt.Errorf("kafka error: %w", err)
	case msg := <-r.messagesChan:
		if msg == nil {
			return nil, fmt.Errorf("EOF")
		}

		item, err := r.deserialize(msg.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize message: %w", err)
		}

		return item, nil
	}
}

func (r *KafkaReader[T]) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	var err error
	if r.useGroup {
		if r.groupConsumer != nil {
			err = r.groupConsumer.Close()
		}
	} else {
		if r.partConsumer != nil {
			if closeErr := r.partConsumer.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		if r.consumer != nil {
			if closeErr := r.consumer.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
	}

	close(r.messagesChan)
	close(r.errorsChan)

	return err
}

func (r *KafkaReader[T]) forwardMessages() {
	for {
		select {
		case msg, ok := <-r.partConsumer.Messages():
			if !ok {
				return
			}
			r.messagesChan <- msg
		case err, ok := <-r.partConsumer.Errors():
			if !ok {
				return
			}
			r.errorsChan <- err
		}
	}
}

func (r *KafkaReader[T]) consumeGroup(ctx context.Context) {
	handler := &consumerGroupHandler[T]{
		reader: r,
		ready:  make(chan bool),
	}

	topics := []string{r.topic}

	for {
		if r.closed {
			return
		}

		err := r.groupConsumer.Consume(ctx, topics, handler)
		if err != nil {
			r.errorsChan <- fmt.Errorf("consumer group error: %w", err)
			time.Sleep(time.Second) // Back off before retry
		}

		if ctx.Err() != nil {
			return
		}
	}
}

type consumerGroupHandler[T any] struct {
	reader *KafkaReader[T]
	ready  chan bool
}

func (h *consumerGroupHandler[T]) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler[T]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.reader.messagesChan <- message
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

type KafkaReaderBuilder[T any] struct {
	config      KafkaReaderConfig
	deserialize func([]byte) (*T, error)
}

func NewKafkaReaderBuilder[T any](brokers []string, topic string) *KafkaReaderBuilder[T] {
	return &KafkaReaderBuilder[T]{
		config: KafkaReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: 0,
			Offset:    sarama.OffsetNewest,
		},
	}
}

func (b *KafkaReaderBuilder[T]) Partition(partition int32) *KafkaReaderBuilder[T] {
	b.config.Partition = partition
	return b
}

func (b *KafkaReaderBuilder[T]) Offset(offset int64) *KafkaReaderBuilder[T] {
	b.config.Offset = offset
	return b
}

func (b *KafkaReaderBuilder[T]) FromBeginning() *KafkaReaderBuilder[T] {
	b.config.Offset = sarama.OffsetOldest
	return b
}

func (b *KafkaReaderBuilder[T]) FromEnd() *KafkaReaderBuilder[T] {
	b.config.Offset = sarama.OffsetNewest
	return b
}

func (b *KafkaReaderBuilder[T]) WithConsumerGroup(groupID string) *KafkaReaderBuilder[T] {
	b.config.GroupID = groupID
	b.config.UseConsumerGroup = true
	return b
}

func (b *KafkaReaderBuilder[T]) WithConfig(config *sarama.Config) *KafkaReaderBuilder[T] {
	b.config.SaramaConfig = config
	return b
}

func (b *KafkaReaderBuilder[T]) WithDeserializer(deserialize func([]byte) (*T, error)) *KafkaReaderBuilder[T] {
	b.deserialize = deserialize
	return b
}

func (b *KafkaReaderBuilder[T]) WithJSONDeserializer() *KafkaReaderBuilder[T] {
	b.deserialize = func(data []byte) (*T, error) {
		var item T
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
		return &item, nil
	}
	return b
}

func (b *KafkaReaderBuilder[T]) Build() (*KafkaReader[T], error) {
	if b.deserialize == nil {
		return nil, fmt.Errorf("deserializer is required (use WithDeserializer or WithJSONDeserializer)")
	}
	return NewKafkaReader[T](b.config, b.deserialize)
}

func (b *KafkaReaderBuilder[T]) BuildJSON() (*KafkaReader[T], error) {
	return NewKafkaJSONReader[T](b.config)
}
