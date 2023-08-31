package models

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
)

type StreamConsumer interface {
	FetchNext() (*FileSection, error)
}

type NatsFileSectionsStreamConsumer struct {
	natsConsumer jetstream.Consumer
}

func NewFileSectionsStreamConsumer(consumer jetstream.Consumer) *NatsFileSectionsStreamConsumer {
	return &NatsFileSectionsStreamConsumer{
		natsConsumer: consumer,
	}
}

func (sc *NatsFileSectionsStreamConsumer) FetchNext() (*FileSection, error) {
	msg, err := sc.natsConsumer.Next()

	if err != nil {
		err := fmt.Errorf("error getting Next FileSection: %v", err)
		return nil, err
	}

	err = msg.Ack()

	if err != nil {
		err := fmt.Errorf("error on ACK of FileSection: %v", err)
		return nil, err
	}

	var fileSection FileSection

	// Unmarshal the JSON data into the Person struct
	err = json.Unmarshal([]byte(msg.Data()), &fileSection)
	if err != nil {
		err := fmt.Errorf("error unmarshaling JSON: %v", err)
		return nil, err
	}

	return &fileSection, nil
}
