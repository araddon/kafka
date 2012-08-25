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

type MessageSender func(msg *MessageTopic)

// an interface for a partitioner that chooses from available partitions
type Partitioner func(*Broker) int

// a produce request with multiple partitions
type ProduceRequest map[string]map[int][]*MessageTopic

// does this ProduceRequest contain more than one topic/partition combo?
func (p ProduceRequest) MultiPart() bool {
    if len(p) > 1 {
      // > 1 topic
      return true
    } else {
      // one topic, how many partitions?
      for _, partMsgs := range p {
        if len(partMsgs) > 1 {
          return true
        }
      }
    }
    return false
}

func (p ProduceRequest) TopicPartCt() (ct int) {
  for _, partMsgs := range p {
    for _, msgs := range partMsgs {
      if len(msgs) > 0 {
        ct ++
      }
    }
  }
  return
}

//type MessageSender func(topic string, partition int, *Message)

type BrokerPublisher struct {
  broker *Broker
}
func NewBrokerPublisher(hostname string, topic string, partition int) *BrokerPublisher {
  tp := TopicPartition{Topic:topic,Partition:partition}
  b := newBroker(hostname,  &tp) 
  return &BrokerPublisher{broker: b}
}
func NewProducer(hostname string, tplist []*TopicPartition) *BrokerPublisher {
  b := newMultiBroker(hostname,  tplist) 
  return &BrokerPublisher{broker: b}
}

// create a new Producer, that uses multi-produce, and random partitioner
func NewPartitionedProducer(hostname string, topic string, partitions []int) *BrokerPublisher {
  b := NewRandomPartitionedBroker(hostname, topic, partitions )
  return &BrokerPublisher{broker: b}
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

  request := b.broker.EncodeProduceRequest(messages...)
  num, err := conn.Write(request)
  if err != nil {
    return -1, err
  }

  return num, err
}

// opens a channel for publishing, blocking call
func (b *BrokerPublisher) PublishOnChannel(msgChan chan *MessageTopic, bufferMaxMs int64, bufferMaxSize int, quit chan bool) error {

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
// uses a partitioner to choose partition
func NewBufferedSender(broker *Broker, bufferMaxMs int64, bufferMaxSize int) (MessageSender, *net.TCPConn, error) {

  conn, err := broker.connect()
  if err != nil {
    return nil, nil, err
  }

  msgBuffer := make(ProduceRequest)
  var hasSent bool
  var msgCt int
  var defaultTopic string
  if len(broker.topics) < 2 {
    defaultTopic = broker.topics[0].Topic
  }
  msgMu := new(sync.Mutex)
  timer := time.NewTicker(time.Duration(bufferMaxMs) * time.Millisecond)

  doSend := func(msgBufCopy ProduceRequest) {

    msgMu.Lock()
    msgCt = 0
    msgBuffer = make(ProduceRequest)
    msgMu.Unlock()
    var err error
    //if msgBufCopy.MultiPart() {
      request := broker.EncodeMultiProduceRequest(&msgBufCopy)
      _, err = conn.Write(request)
    //} else {
    //  for _, partMsgs := range msgBufCopy {
    //    for _, msgs := range partMsgs {
    //      request := broker.EncodeProduceRequest(msgs...)
    //      _, err = conn.Write(request)
    //    }
    //  }
    //}

    if err != nil {
      // TODO, reconnect?  
      log.Println("potentially fatal error?", err)
    }
  }

  log.Println("start buffered sender heartbeat = ", bufferMaxMs, " max queue ", bufferMaxSize)
  go func() {

    for _ = range timer.C {
      msgMu.Lock()
      if msgCt > 0 && !hasSent {
        hasSent = false
        msgMu.Unlock()
        doSend(msgBuffer)
      } else {
        msgMu.Unlock()
      }
      
    }

  }()

  return func(msg *MessageTopic) {
    if msg == nil {
      doSend(msgBuffer)
      return
    }
    msgMu.Lock()
    msgCt++
    partId := msg.Partition
    topic := defaultTopic
    if partId == -1 {
      // get a partition
      partId = broker.Partitioner(broker)
    }
    if len(msg.Topic) > 0 {
      topic = msg.Topic
    }
    //log.Println("partid = ", partId)
    if _, ok := msgBuffer[topic]; !ok {
      msgBuffer[topic] = make(map[int][]*MessageTopic)
    }
    if _, ok := msgBuffer[topic][partId]; !ok {
      msgBuffer[topic][partId] = []*MessageTopic{}
    } 

    msgBuffer[topic][partId] = append(msgBuffer[topic][partId], msg)
    if msgCt > bufferMaxSize {
      hasSent = true
      msgMu.Unlock()
      go doSend(msgBuffer)
    } else {
      msgMu.Unlock()
    }

  }, conn, nil
}
