/*
 *  Copyright (c) 2011 NeuStar, Inc.
 *  All rights reserved.  
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at 
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  NeuStar, the Neustar logo and related names and logos are registered
 *  trademarks, service marks or tradenames of NeuStar, Inc. All other 
 *  product names, company names, marks, logos and symbols may be trademarks
 *  of their respective owners.
 */

package kafka

import (
  "log"
  "net"
  "sync"
  "time"
)

type MessageSender func(*Message)

type BrokerPublisher struct {
  broker *Broker
}

func NewBrokerPublisher(hostname string, topic string, partition int) *BrokerPublisher {
  return &BrokerPublisher{broker: newBroker(hostname, topic, partition)}
}

func (b *BrokerPublisher) Publish(message *Message) (int, error) {
  return b.BatchPublish(message)
}

func (b *BrokerPublisher) BatchPublish(messages ...*Message) (int, error) {
  conn, err := b.broker.connect()
  if err != nil {
    return -1, err
  }
  defer conn.Close()
  // TODO: MULTIPRODUCE
  request := b.broker.EncodePublishRequest(messages...)
  num, err := conn.Write(request)
  if err != nil {
    return -1, err
  }

  return num, err
}

// opens a channel for publishing, blocking call
func (b *BrokerPublisher) PublishOnChannel(msgChan chan *Message, bufferMaxMs int64, bufferMaxSize int, quit chan bool) error {

  sender, conn, err := NewBufferedSender(b.broker, bufferMaxMs, bufferMaxSize)

  if err == nil {

    // wait for stop signal
    go func() {
      <-quit
      sender(nil) // flush
      conn.Close()
      close(msgChan)
    }()

    for msg := range msgChan {
      if msg != nil {
        sender(msg)
      }
    }

  }
  return err

}

// Buffered Sender, buffers messages for max time, and max size
func NewBufferedSender(broker *Broker, bufferMaxMs int64, bufferMaxSize int) (MessageSender, *net.TCPConn, error) {

  conn, err := broker.connect()
  if err != nil {
    return nil, nil, err
  }

  msgBuffer := make([]*Message, 0)

  var hasSent bool

  msgMu := new(sync.Mutex)
  timer := time.NewTicker(time.Duration(bufferMaxMs) * time.Millisecond)

  doSend := func() {
    msgMu.Lock()
    var msgBufCopy []*Message
    msgBufCopy = msgBuffer
    msgBuffer = make([]*Message, 0)
    msgMu.Unlock()
    request := broker.EncodePublishRequest(msgBufCopy...)
    _, err := conn.Write(request)
    if err != nil {
      // ? panic?
      log.Println("potentially fatal error?")
    }
  }

  log.Println("start buffered sender heartbeat = ", bufferMaxMs, " max queue ", bufferMaxSize)
  go func() {

    for _ = range timer.C {
      msgMu.Lock()
      if len(msgBuffer) > 0 && !hasSent {
        msgMu.Unlock()
        doSend()
      } else {
        msgMu.Unlock()
      }
      hasSent = false
    }

  }()

  return func(msg *Message) {
    if msg == nil {
      doSend()
      return
    }
    msgMu.Lock()
    msgBuffer = append(msgBuffer, msg)
    if len(msgBuffer) >= bufferMaxSize {
      hasSent = true
      msgMu.Unlock()
      go doSend()
    } else {
      msgMu.Unlock()
    }

  }, conn, nil
}
