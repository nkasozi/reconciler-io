package models

import "sync"

type FanOutChannel struct {
	consumers map[string]chan FileSection
	messages  []FileSection
	mu        sync.Mutex
}

func NewFanOutChannel(sourceChan <-chan FileSection) *FanOutChannel {
	fc := &FanOutChannel{
		consumers: make(map[string]chan FileSection),
	}
	fc.listenAndFanOutMessagesAsync(sourceChan)
	return fc
}

func (fc *FanOutChannel) GetNewChannelByID(id string, bufferSize int) chan FileSection {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	ch := make(chan FileSection, bufferSize)
	for _, msg := range fc.messages {
		ch <- msg
	}
	fc.consumers[id] = ch
	return ch
}

func (fc *FanOutChannel) listenAndFanOutMessagesAsync(sourceChan <-chan FileSection) {
	go func() {
		for msg := range sourceChan {
			fc.mu.Lock()
			fc.messages = append(fc.messages, msg)
			for _, ch := range fc.consumers {
				ch <- msg
			}
			fc.mu.Unlock()
		}
	}()
}

func (fc *FanOutChannel) CloseChannelById(id string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	close(fc.consumers[id])
	delete(fc.consumers, id)
}

func (fc *FanOutChannel) SendCloseSignalOnAllChannels() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	for key, consumer := range fc.consumers {
		close(consumer)
		delete(fc.consumers, key)
	}

	fc.messages = make([]FileSection, 0)
}
