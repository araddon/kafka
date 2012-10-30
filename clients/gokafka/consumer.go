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
	//"encoding/binary"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

type MessageHandlerFunc func(string, int, *Message)

type BrokerConsumer struct {
	broker  *Broker
	codecs  map[byte]PayloadCodec
	Handler MessageHandlerFunc
}

// Create a new broker consumer
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
// offset to start consuming from
// maxSize (in bytes) of the message to consume (this should be at least as big as the biggest message to be published)
func NewBrokerConsumer(hostname string, topic string, partition int, offset uint64, maxSize uint32) *BrokerConsumer {
	tp := TopicPartition{Topic: topic, Partition: partition, Offset: offset, MaxSize: maxSize}
	return &BrokerConsumer{broker: newBroker(hostname, &tp), codecs: DefaultCodecsMap}
}

// Multiple Topic/Partition consumer
func NewMultiConsumer(hostname string, tplist []*TopicPartition) *BrokerConsumer {
	//[]*TopicPartition{tp}
	return &BrokerConsumer{broker: newMultiBroker(hostname, tplist), codecs: DefaultCodecsMap}
}

// Multiple Topic/Partition consumer, one topic but many partitions
func NewConsumerPartitions(hostname string, topic string, partitions []int, offset uint64, maxSize uint32) *BrokerConsumer {
	tplist := make([]*TopicPartition, len(partitions))
	for tpi, part := range partitions {
		tplist[tpi] = &TopicPartition{Topic: topic, Partition: part, Offset: offset, MaxSize: maxSize}
	}
	return &BrokerConsumer{broker: newMultiBroker(hostname, tplist), codecs: DefaultCodecsMap}
}

// Simplified consumer that defaults the offset and maxSize to 0.
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
func NewBrokerOffsetConsumer(hostname string, topic string, partition int) *BrokerConsumer {
	tp := TopicPartition{Topic: topic, Partition: partition, Offset: 0, MaxSize: 0}
	return &BrokerConsumer{broker: newBroker(hostname, &tp), codecs: DefaultCodecsMap}
}

// Add Custom Payload Codecs for Consumer Decoding
// payloadCodecs - an array of PayloadCodec implementations
func (consumer *BrokerConsumer) AddCodecs(payloadCodecs []PayloadCodec) {
	// merge to the default map, so one 'could' override the default codecs..
	for k, v := range codecsMap(payloadCodecs) {
		consumer.codecs[k] = v
	}
}

func (consumer *BrokerConsumer) handleConnError(err error, conn *net.TCPConn) error {
	errs := err.Error()
	if strings.HasSuffix(errs, "broken pipe") {
		for i := 0; i < 100; i++ {
			log.Println("Reconnecting")
			conn, err = consumer.broker.connect()
			if err == nil {
				return nil
			}
			time.Sleep(time.Millisecond * 1000)
		}
	}
	return err
}
func (consumer *BrokerConsumer) ConsumeOnChannel(msgChan chan *Message, pollTimeoutMs int64, quit chan bool) (int, error) {
	conn, err := consumer.broker.connect()
	time.Sleep(time.Duration(pollTimeoutMs) * time.Millisecond * 2)
	if err != nil {
		quit <- true
		return -1, err
	}

	num := 0
	errCt := 0
	done := make(chan bool, 1)
	isDone := false
	go func() {
		for {
			if isDone {
				return
			}
			//tp := consumer.broker.topics[0]
			//log.Println("about to poll for consume ", pollTimeoutMs)
			_, err := consumer.consumeWithConn(conn, func(topic string, partition int, msg *Message) {
				msgChan <- msg
				num += 1
			})

			if err != nil {
				if err != io.EOF {
					log.Println("Fatal Error: ", err)
					errCt++
					//panic(err)
					//quit <- true // force quit
				}
			} else {
				errCt -= 2
			}
			if errCt > 50 {
				panic(err)
			}

			time.Sleep(time.Duration(pollTimeoutMs) * time.Millisecond)
		}
		//log.Println("got done signal in loop1")
		done <- true
		//log.Println("got done signal in loop2")
	}()
	// wait to be told to stop..
	<-quit
	isDone = true
	log.Println("got quit signal, clossing conn")
	conn.Close()
	close(msgChan)
	done <- true
	return num, err
}

func (consumer *BrokerConsumer) Consume(handlerFunc MessageHandlerFunc) (int, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	num, err := consumer.consumeWithConn(conn, handlerFunc)

	if err != nil {
		log.Println("Fatal Error: ", err)
	}

	return num, err
}

func (consumer *BrokerConsumer) tryConnect(conn *net.TCPConn, tp *TopicPartition) (err error, reader *ByteBuffer) {
	var errCode int
	request := consumer.broker.EncodeConsumeRequest()
	//log.Println("offset=", tp.Offset, " ", tp.MaxSize, " ", request, " ", tp.Topic, " ", tp.Partition, "  \n\t", string(request))
	_, err = conn.Write(request)
	if err != nil {
		if err = consumer.handleConnError(err, conn); err != nil {
			return err, nil
		}
	}

	reader = consumer.broker.readResponse(conn)
	err, errCode = reader.ReadHeader()
	if err != nil && errCode == 1 {
		offsetVal := getOffset(consumer.broker.hostname, tp)
		if offsetVal > 0 {
			// RECONNECT!
			log.Println("RECONNECTING !!! ", offsetVal)
			tp.Offset = offsetVal
			if err, reader = consumer.tryConnect(conn, tp); err != nil {
				return err, nil
			}
		} else {
			return err, nil
		}

	} else if err != nil {
		log.Println("offset=", tp.Offset, " ", tp.MaxSize, " ", request, " ", tp.Topic, " ", tp.Partition, "  \n\t", string(request))
		return err, nil
	}
	return
}

func (consumer *BrokerConsumer) consumeWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc) (num int, err error) {

	var msgs []*Message
	var payloadConsumed int
	var reader *ByteBuffer

	if len(consumer.broker.topics) > 1 {
		return consumer.consumeMultiWithConn(conn, handlerFunc)
	}

	tp := consumer.broker.topics[0]
	if err, reader = consumer.tryConnect(conn, tp); err != nil {
		return -1, err
	}

	//log.Println(reader.)
	if reader.Size > 2 {
		// parse out the messages
		var currentOffset uint64 = 0
		for {
			//log.Println("before nextMsg ", currentOffset)
			payloadConsumed, msgs, err = reader.NextMsg(consumer.codecs)
			//log.Println("after nxt msg", len(msgs), " ", payloadConsumed, " ", currentOffset)
			if err != nil {
				log.Println("ERROR< ", err)
			}
			if msgs == nil || len(msgs) == 0 {
				// this isn't invalid as net conn bytes might contain partial messages 
				tp.Offset += currentOffset
				//log.Println("end of message set ", tp.Offset, " ", currentOffset)
				return num, err
			}
			msgOffset := tp.Offset + currentOffset

			for _, msg := range msgs {
				// update all of the messages offset
				// multiple messages can be at the same offset (compressed for example)
				msg.offset = msgOffset
				//msgOffset += 4 + uint64(msg.totalLength)
				msgOffset += msg.TotalLen()
				//log.Println("about to call handler func ", msgOffset)
				handlerFunc(tp.Topic, tp.Partition, msg)
				//log.Println("after handler func")
				num += 1
			}

			currentOffset += uint64(payloadConsumed)
		}
		// update the topic/partition segment offset for next consumption
		tp.Offset += currentOffset
	}

	return num, err
}

func (consumer *BrokerConsumer) consumeMultiWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc) (num int, err error) {

	var errCode int
	_, err = conn.Write(consumer.broker.EncodeConsumeRequestMultiFetch())

	if err != nil {
		if err = consumer.handleConnError(err, conn); err != nil {
			return -1, err
		}
	}
	log.Println("about to call read multi response")
	reader := consumer.broker.readMultiResponse(conn)
	log.Println("after call")
	err, errCode = reader.ReadHeader()
	log.Println("after read header", err)
	if err != nil && errCode == 1 {
		// RECONNECT!
		log.Println("ERROR, bad offsetIds")
		return -1, err
	} else if err != nil {
		log.Println("Fatal Error: ", err)
		return -1, err
	}

	var tp *TopicPartition
	var currentOffset uint64
	var msgs []*Message
	var payloadConsumed int

	log.Println("len ", reader.Len())
	for tpi := 0; tpi < reader.Len(); tpi++ {
		//log.Println("new loop ", tpi)
		// do we not know the topic/partition?  or assume it stayed ordered?
		tp = consumer.broker.topics[tpi]

		length, err := reader.ReadSet()
		log.Println("size of this set", length)
		if err != nil || reader == nil {
			log.Println("ERROR, err in read", err)
			return -1, err
		}
		currentOffset = 0
	msgsetloop:
		for {
			payloadConsumed, msgs, err = reader.NextMsg(consumer.codecs)
			log.Println("consumed", payloadConsumed, currentOffset)
			if err != nil {
				log.Println("ERROR< ", err)
				break
			}
			if msgs == nil || len(msgs) == 0 {
				// this isn't invalid as large messages might contain partial messages 
				tp.Offset += currentOffset
				log.Println("no messages? ")
				return num, err
			}
			msgOffset := tp.Offset + currentOffset

			for _, msg := range msgs {
				// update all of the messages offset
				// multiple messages can be at the same offset (compressed for example)
				msg.offset = msgOffset
				//msgOffset += 4 + uint64(msg.totalLength)
				msgOffset += msg.TotalLen()
				handlerFunc(tp.Topic, tp.Partition, msg)
				num += 1
			}

			currentOffset += uint64(payloadConsumed)
			log.Println(currentOffset, length)
			if currentOffset+2 >= uint64(length) {
				break msgsetloop
			}
		}
		// update the topic/partition segment offset for next consumption
		if currentOffset > 2 {
			//currentOffset +=2
			tp.Offset += currentOffset
		}

		log.Println("tp.offset ", tp.Offset, tp.Partition)
	}
	return num, err
}

// Get a list of valid offsets (up to maxNumOffsets) before the given time, where 
// time is in milliseconds (-1, from the latest offset available, -2 from the smallest offset available)
// The result is a list of offsets, in descending order.
func (consumer *BrokerConsumer) GetOffsets(time int64, maxNumOffsets uint32) ([]uint64, error) {
	offsets := make([]uint64, 0)

	conn, err := consumer.broker.connect()
	if err != nil {
		log.Println("ERROR ", err)
		return offsets, err
	}

	defer conn.Close()

	offsetRequest := consumer.broker.EncodeOffsetRequest(time, maxNumOffsets)
	_, err = conn.Write(offsetRequest)
	if err != nil {
		log.Println("ERROR ", err)
		return offsets, err
	}

	reader := consumer.broker.readResponse(conn)
	err, _ = reader.ReadHeader()
	if err != nil {
		log.Println("HEADER ERROR ", err)
		return offsets, err
	}
	offsets, err = reader.Offsets()
	//log.Println("offsets Ct= ", len(offsets), " size=", reader.Size)
	if err != nil {
		log.Println("ERROR ", err)
		return offsets, err
	}

	return offsets, err
}

func getOffset(hostname string, tp *TopicPartition) uint64 {
	broker := NewBrokerOffsetConsumer(hostname, tp.Topic, tp.Partition)

	offsets, err := broker.GetOffsets(-2, uint32(1))
	if err != nil {
		log.Println("Error: ", err)
	}
	if len(offsets) == 1 {
		return offsets[0]
	}
	return 0
}
