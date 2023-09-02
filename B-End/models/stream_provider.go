package models

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StreamProvider interface {
	SetupStream(ctx context.Context, streamName string, topicName string) error
	DeleteStreamTopic(ctx context.Context, streamName string, topicName string) error
	PublishToTopic(ctx context.Context, topicName string, data interface{}) error
	CreateStreamConsumer(ctx context.Context, streamName, topicName, consumerName string) (StreamConsumer, error)
	DeleteStreamConsumer(ctx context.Context, streamName string, consumerName string) error
}

type NatsStreamProvider struct {
	channel jetstream.JetStream
}

func NewStreamProvider(natsUrl string) (StreamProvider, error) {
	connected, err := connect(natsUrl)
	if err != nil {
		return nil, err
	}
	streamChannel := NatsStreamProvider{channel: connected}
	return &streamChannel, nil
}

func connect(natsUrl string) (jetstream.JetStream, error) {
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return js, nil
}

func (sc *NatsStreamProvider) SetupStream(ctx context.Context, streamName string, topicName string) error {
	// Try to get the existing stream
	stream, err := sc.channel.Stream(ctx, streamName)

	// If the stream exists
	if err == nil {
		// Check if the subject is already part of the stream
		for _, subject := range stream.CachedInfo().Config.Subjects {
			if subject == topicName {
				// Subject already exists, no need to update the stream
				return nil
			}
		}

		// Update the stream to add the new subject
		_, err = sc.channel.UpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: append(stream.CachedInfo().Config.Subjects, topicName),
		})
		if err != nil {
			return fmt.Errorf("failed to update stream: %w", err)
		}
		return nil
	}

	// If the stream does not exist, create a new one
	_, err = sc.channel.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{topicName},
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

func (sc *NatsStreamProvider) DeleteStreamTopic(ctx context.Context, streamName string, topicName string) error {
	// try to get the existing stream
	stream, err := sc.channel.Stream(ctx, streamName)

	// if the stream doesn't exist,
	// we don't need to do anything
	if err != nil {
		return nil
	}

	// Update the stream to delete the  subject
	_, err = sc.channel.UpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: deleteByValue(stream.CachedInfo().Config.Subjects, topicName),
	})

	// failed to delete
	if err != nil {
		return fmt.Errorf("failed to update stream: %w", err)
	}

	//success
	return nil
}

func deleteByValue(subjects []string, name string) []string {
	for i, v := range subjects {
		if v == name {
			return append(subjects[:i], subjects[i+1:]...)
		}
	}
	return subjects
}

func (sc *NatsStreamProvider) PublishToTopic(
	ctx context.Context,
	topicName string,
	data interface{},
) error {
	// Convert the FileToBeRead to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Publish the message to a subject
	_, err = sc.channel.Publish(
		ctx,
		topicName,
		jsonData,
	)
	return err
}

func (sc *NatsStreamProvider) CreateStreamConsumer(
	ctx context.Context,
	streamName string,
	topicName string,
	consumerName string,
) (StreamConsumer, error) {
	cons, err := sc.channel.CreateOrUpdateConsumer(
		ctx,
		streamName,
		jetstream.ConsumerConfig{
			Durable:        consumerName,
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{topicName},
		})
	if err != nil {
		return nil, err
	}
	return NewFileSectionsStreamConsumer(cons), nil
}

func (sc *NatsStreamProvider) DeleteStreamConsumer(
	ctx context.Context,
	streamName string,
	consumerName string,
) error {
	err := sc.channel.DeleteConsumer(
		ctx,
		streamName,
		consumerName,
	)
	return err
}
