package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type KafkaWriter[T any] struct {
	producer    sarama.SyncProducer
	asyncProd   sarama.AsyncProducer
	topic       string
	async       bool
	serialize   func(T) ([]byte, error)
	keyFunc     func(T) ([]byte, error)
	successChan chan *sarama.ProducerMessage
	errorsChan  chan *sarama.ProducerError
	closed      bool
}

type KafkaWriterConfig struct {
	Brokers         []string
	Topic           string
	Async           bool
	RequiredAcks    sarama.RequiredAcks
	Compression     sarama.CompressionCodec
	MaxMessageBytes int
	RetryMax        int
	SaramaConfig    *sarama.Config
}

func NewKafkaWriter[T any](config KafkaWriterConfig, serialize func(T) ([]byte, error)) (*KafkaWriter[T], error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if serialize == nil {
		return nil, fmt.Errorf("serialize function is required")
	}

	saramaConfig := config.SaramaConfig
	if saramaConfig == nil {
		saramaConfig = sarama.NewConfig()
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas
		saramaConfig.Producer.Retry.Max = 3
		saramaConfig.Producer.Return.Successes = true
		saramaConfig.Producer.Return.Errors = true
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
		saramaConfig.Version = sarama.V2_6_0_0
	}

	if config.RequiredAcks != 0 {
		saramaConfig.Producer.RequiredAcks = config.RequiredAcks
	}
	if config.Compression != 0 {
		saramaConfig.Producer.Compression = config.Compression
	}
	if config.MaxMessageBytes > 0 {
		saramaConfig.Producer.MaxMessageBytes = config.MaxMessageBytes
	}
	if config.RetryMax > 0 {
		saramaConfig.Producer.Retry.Max = config.RetryMax
	}

	writer := &KafkaWriter[T]{
		topic:     config.Topic,
		async:     config.Async,
		serialize: serialize,
	}

	var err error
	if config.Async {
		writer.asyncProd, err = sarama.NewAsyncProducer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create async producer: %w", err)
		}

		writer.successChan = make(chan *sarama.ProducerMessage, 100)
		writer.errorsChan = make(chan *sarama.ProducerError, 100)

		go writer.monitorAsync()
	} else {
		writer.producer, err = sarama.NewSyncProducer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync producer: %w", err)
		}
	}

	return writer, nil
}

func NewKafkaJSONWriter[T any](config KafkaWriterConfig) (*KafkaWriter[T], error) {
	serialize := func(item T) ([]byte, error) {
		data, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %w", err)
		}
		return data, nil
	}
	return NewKafkaWriter[T](config, serialize)
}

func (w *KafkaWriter[T]) Write(ctx context.Context, items []T) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if len(items) == 0 {
		return nil
	}

	if w.async {
		return w.writeAsync(ctx, items)
	}
	return w.writeSync(ctx, items)
}

func (w *KafkaWriter[T]) writeSync(ctx context.Context, items []T) error {
	for i, item := range items {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled after %d/%d items: %w", i, len(items), ctx.Err())
		default:
		}

		data, err := w.serialize(item)
		if err != nil {
			return fmt.Errorf("failed to serialize item %d: %w", i, err)
		}

		msg := &sarama.ProducerMessage{
			Topic: w.topic,
			Value: sarama.ByteEncoder(data),
		}

		if w.keyFunc != nil {
			key, err := w.keyFunc(item)
			if err != nil {
				return fmt.Errorf("failed to generate key for item %d: %w", i, err)
			}
			msg.Key = sarama.ByteEncoder(key)
		}

		partition, offset, err := w.producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}

		_ = partition
		_ = offset
	}

	return nil
}

func (w *KafkaWriter[T]) writeAsync(ctx context.Context, items []T) error {
	errorCount := 0
	successCount := 0
	totalItems := len(items)

	// Send all messages
	for i, item := range items {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled after %d/%d items: %w", i, totalItems, ctx.Err())
		default:
		}

		data, err := w.serialize(item)
		if err != nil {
			return fmt.Errorf("failed to serialize item %d: %w", i, err)
		}

		msg := &sarama.ProducerMessage{
			Topic: w.topic,
			Value: sarama.ByteEncoder(data),
		}

		if w.keyFunc != nil {
			key, err := w.keyFunc(item)
			if err != nil {
				return fmt.Errorf("failed to generate key for item %d: %w", i, err)
			}
			msg.Key = sarama.ByteEncoder(key)
		}

		w.asyncProd.Input() <- msg
	}

	timeout := time.After(30 * time.Second)

	for successCount+errorCount < totalItems {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %d succeeded, %d failed: %w",
				successCount, errorCount, ctx.Err())
		case <-timeout:
			return fmt.Errorf("timeout waiting for acknowledgments: %d succeeded, %d failed",
				successCount, errorCount)
		case <-w.successChan:
			successCount++
		case err := <-w.errorsChan:
			errorCount++
			if errorCount == 1 {
				return fmt.Errorf("async write error: %w", err.Err)
			}
		}
	}

	if errorCount > 0 {
		return fmt.Errorf("async write completed with %d errors out of %d items",
			errorCount, totalItems)
	}

	return nil
}

// monitorAsync monitors async producer for successes and errors.
func (w *KafkaWriter[T]) monitorAsync() {
	for {
		select {
		case success, ok := <-w.asyncProd.Successes():
			if !ok {
				return
			}
			w.successChan <- success
		case err, ok := <-w.asyncProd.Errors():
			if !ok {
				return
			}
			w.errorsChan <- err
		}
	}
}

func (w *KafkaWriter[T]) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	var err error
	if w.async {
		if w.asyncProd != nil {
			err = w.asyncProd.Close()
			close(w.successChan)
			close(w.errorsChan)
		}
	} else {
		if w.producer != nil {
			err = w.producer.Close()
		}
	}

	return err
}

func (w *KafkaWriter[T]) WithKey(keyFunc func(T) ([]byte, error)) *KafkaWriter[T] {
	w.keyFunc = keyFunc
	return w
}

type KafkaWriterBuilder[T any] struct {
	config    KafkaWriterConfig
	serialize func(T) ([]byte, error)
	keyFunc   func(T) ([]byte, error)
}

func NewKafkaWriterBuilder[T any](brokers []string, topic string) *KafkaWriterBuilder[T] {
	return &KafkaWriterBuilder[T]{
		config: KafkaWriterConfig{
			Brokers:         brokers,
			Topic:           topic,
			Async:           false,
			RequiredAcks:    sarama.WaitForAll,
			Compression:     sarama.CompressionSnappy,
			MaxMessageBytes: 1000000, // 1MB
			RetryMax:        3,
		},
	}
}

func (b *KafkaWriterBuilder[T]) Async(async bool) *KafkaWriterBuilder[T] {
	b.config.Async = async
	return b
}

func (b *KafkaWriterBuilder[T]) RequiredAcks(acks sarama.RequiredAcks) *KafkaWriterBuilder[T] {
	b.config.RequiredAcks = acks
	return b
}

func (b *KafkaWriterBuilder[T]) Compression(codec sarama.CompressionCodec) *KafkaWriterBuilder[T] {
	b.config.Compression = codec
	return b
}

func (b *KafkaWriterBuilder[T]) MaxMessageBytes(bytes int) *KafkaWriterBuilder[T] {
	b.config.MaxMessageBytes = bytes
	return b
}

func (b *KafkaWriterBuilder[T]) RetryMax(max int) *KafkaWriterBuilder[T] {
	b.config.RetryMax = max
	return b
}

func (b *KafkaWriterBuilder[T]) WithConfig(config *sarama.Config) *KafkaWriterBuilder[T] {
	b.config.SaramaConfig = config
	return b
}

func (b *KafkaWriterBuilder[T]) WithSerializer(serialize func(T) ([]byte, error)) *KafkaWriterBuilder[T] {
	b.serialize = serialize
	return b
}

func (b *KafkaWriterBuilder[T]) WithJSONSerializer() *KafkaWriterBuilder[T] {
	b.serialize = func(item T) ([]byte, error) {
		data, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %w", err)
		}
		return data, nil
	}
	return b
}

func (b *KafkaWriterBuilder[T]) WithKey(keyFunc func(T) ([]byte, error)) *KafkaWriterBuilder[T] {
	b.keyFunc = keyFunc
	return b
}

func (b *KafkaWriterBuilder[T]) Build() (*KafkaWriter[T], error) {
	if b.serialize == nil {
		return nil, fmt.Errorf("serializer is required (use WithSerializer or WithJSONSerializer)")
	}

	writer, err := NewKafkaWriter[T](b.config, b.serialize)
	if err != nil {
		return nil, err
	}

	if b.keyFunc != nil {
		writer.WithKey(b.keyFunc)
	}

	return writer, nil
}

func (b *KafkaWriterBuilder[T]) BuildJSON() (*KafkaWriter[T], error) {
	b.WithJSONSerializer()
	return b.Build()
}
