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
  "bytes"
  "encoding/binary"
  "log"
)

type RequestType uint16

// Request Types
const (
  REQUEST_PRODUCE      RequestType = 0
  REQUEST_FETCH                    = 1
  REQUEST_MULTIFETCH               = 2
  REQUEST_MULTIPRODUCE             = 3
  REQUEST_OFFSETS                  = 4
)

// Request Header: <REQUEST_SIZE: uint32><REQUEST_TYPE: uint16>
func (b *Broker) EncodeRequestHeader(request *bytes.Buffer, requestType RequestType) {
  request.Write(uint32bytes(0)) // placeholder for request size
  request.Write(uint16bytes(int(requestType)))
}

// Topic/Partition Header: <TOPIC SIZE: uint16><TOPIC: bytes><PARTITION: uint32>
func EncodeTopicHeader(request *bytes.Buffer, topic string, partition int) {
  request.Write(uint16bytes(len(topic)))
  request.WriteString(topic)
  request.Write(uint32bytes(uint32(partition)))
}

// Topic Fetch Request: <TOPICHEADER><OFFSET: uint64><MAX SIZE: uint32>
func EncodeTopicFetch(request *bytes.Buffer, tp *TopicPartition) {
  EncodeTopicHeader(request, tp.Topic, tp.Partition)
  request.Write(uint64ToUint64bytes(tp.Offset))
  request.Write(uint32bytes(tp.MaxSize))
}

// after writing to the buffer is complete, encode the size of the request in the request.
func encodeRequestSize(request *bytes.Buffer) {
  binary.BigEndian.PutUint32(request.Bytes()[0:], uint32(request.Len()-4))
}

// <Request Header><TOPICHEADER><TIME: uint64><MAX NUMBER of OFFSETS: uint32>
func (b *Broker) EncodeOffsetRequest(time int64, maxNumOffsets uint32) []byte {
  request := bytes.NewBuffer([]byte{})
  b.EncodeRequestHeader(request, REQUEST_OFFSETS)

  if len(b.topics) > 1 {
    // TODO: more than one offset if > 1 partition

  } else {
    EncodeTopicHeader(request, b.topics[0].Topic, b.Partitioner(b))
    // specific to offset request
    request.Write(uint64ToUint64bytes(uint64(time)))
    request.Write(uint32toUint32bytes(maxNumOffsets))
  }

  encodeRequestSize(request)

  return request.Bytes()
}

// <Request Header>
//  <TOPICHEADER><OFFSET: uint64><MAX SIZE: uint32>
func (b *Broker) EncodeConsumeRequest() []byte {
  request := bytes.NewBuffer([]byte{})

  //if len(b.topics) != 1 {
  //  return request.Bytes()
  //}
  tp := b.topics[0]

  b.EncodeRequestHeader(request, REQUEST_FETCH)

  EncodeTopicFetch(request, tp)

  encodeRequestSize(request)

  return request.Bytes()
}

// <Request Header>
//  <TOPICPARTITION_COUNT: uint32>
//    <TOPICHEADER><OFFSET: uint64><MAX SIZE: uint32>
//    <TOPICHEADER><OFFSET: uint64><MAX SIZE: uint32>
func (b *Broker) EncodeConsumeRequestMultiFetch() []byte {

  /*

  [0 0 0 48 0 2 0 2 
    0 4 116 101 115 116 0 0 0 0 0 0 0 0 0 0 0 0 0 16 0 0 
    0 4 116 101 115 116 0 0 0 1 0 0 0 0 0 0 0 0 0 16 0 0]
  
  [0 0 0 48 0 2 0 2 
    0 4 116 101 115 116 0 0 0 0 0 0 0 0 0 0 6 137 0 16 0 0
    0 4 116 101 115 116 0 0 0 1 0 0 0 0 0 0 3 97 0 16 0 0]
  
  0 0 0 48         <REQUEST_SIZE: uint32>
  0 2              <REQUEST_TYPE: uint16>
  0 2              <TOPICPARTITION_COUNT: uint16>
    -- repeat
    116 101 115 116  <TOPIC: bytes>
    0 0 0 0          <PARTITION: uint32>
    0 0 0 0 0 0 0 0  <OFFSET: uint64>
    0 0 0 0          <PARTITION: uint32>
  */

  request := bytes.NewBuffer([]byte{})
  b.EncodeRequestHeader(request, REQUEST_MULTIFETCH)
  
  request.Write(uint16bytes(len(b.topics)))

  for _, tp := range b.topics {
    EncodeTopicFetch(request, tp)
  }

  encodeRequestSize(request)
  log.Println(request.Bytes())
  return request.Bytes()
}

// <Request Header><TOPICHEADER><MESSAGE SET SIZE: uint32><MESSAGES>
/*
[0 0 0 24 0 1 0 4 116 101 115 116 0 0 0 0 0 0 0 0 0 0 0 0 0 16 0 0]
[0 0 0 24 0 2 0 4 116 101 115 116 0 0 0 0 0 0 0 0 0 0 0 0 0 16 0 0]
*/
func (b *Broker) EncodeProduceRequest(messages ...*Message) []byte {
  // 4 + 2 + 2 + topicLength + 4 + 4
  request := bytes.NewBuffer([]byte{})
  if len(b.topics) != 1 {
    return request.Bytes()
  }

  b.EncodeRequestHeader(request, REQUEST_PRODUCE)
  EncodeTopicHeader(request, b.topics[0].Topic, b.Partitioner(b))

  messageSetSizePos := request.Len()
  request.Write(uint32bytes(0)) // placeholder message len

  written := 0
  for _, msg := range messages {
    wrote, _ := request.Write(msg.Encode())
    written += wrote
  }

  // now add the accumulated size of that the message set was
  binary.BigEndian.PutUint32(request.Bytes()[messageSetSizePos:], uint32(written))

  // now add the size of the whole to the first uint32
  encodeRequestSize(request)
  return request.Bytes()
}

// <Request Header>
//    <topic-partition count:  uint32>
//    <TOPICHEADER>
//      <MESSAGE SET SIZE: uint32><MESSAGES>
//    <TOPICHEADER>
//      <MESSAGE SET SIZE: uint32><MESSAGES>
//    .....
func (b *Broker) EncodeMultiProduceRequest(preq *ProduceRequest) []byte {

  /*
  Format of a Multi-Produce Request
  [0 0 0 92 0 3 0 2 
    0 4 116 101 115 116 0 0 0 1 0 0 0 15 
      0 0 0 11 1 0 254 240 190 143 97 97 114 111 110
    0 4 116 101 115 116 0 0 0 0 0 0 0 45 
      0 0 0 11 1 0 254 240 190 143 97 97 114 111 110 
      0 0 0 11 1 0 254 240 190 143 97 97 114 111 110 
      0 0 0 11 1 0 254 240 190 143 97 97 114 111 110]
  
  0 0 0 92         <REQUEST_SIZE: uint32>
  0 3              <REQUEST_TYPE: uint16>
  0 2              <TOPICPARTITION_COUNT: uint16>
    -- repeat
    0 4              <TOPIC_LEN: uint16>
    116 101 115 116  <TOPIC: bytes>
    0 0 0 1          <PARTITION: uint32>
    0 0 0 15         <MESSAGE SET SIZE: uint32>
      - repeat
      0 0 0 11         <MESSAGE SIZE: uint32>
      1                <magic>
      0                <compression>
      254 240 190 143      <checksum>
      97 97 114 111 110 <payload>
  */
  request := bytes.NewBuffer([]byte{})

  b.EncodeRequestHeader(request, REQUEST_MULTIPRODUCE)
  request.Write(uint16bytes(preq.TopicPartCt()))
  
  for topic, partMsgs := range *preq {
    for partition, messages := range partMsgs {
      
      // TODO, support multiple topics (right now, just multiple partitions for single topic)
      EncodeTopicHeader(request, topic, partition)

      messageSetSizePos := request.Len()
      request.Write(uint32bytes(0)) // placeholder message len

      written := 0
      for _, message := range messages {
        wrote, _ := request.Write(message.Message.Encode())
        written += wrote
      }
      
      // now add the accumulated size of that the message set was
      binary.BigEndian.PutUint32(request.Bytes()[messageSetSizePos:], uint32(written))

    }
  }

  encodeRequestSize(request)
  
  return request.Bytes()
}

