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
	"bufio"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	NETWORK = "tcp"
)

type TopicPartition struct {
	Offset    uint64
	MaxSize   uint32
	Topic     string
	Partition int
}

// creates a list of Topic Partitions for a single topic
func NewTopicPartitions(topic, partstr string, offset uint64, maxSize uint32) []*TopicPartition {
	parts := strings.Split(partstr, ",")
	partitions := make([]*TopicPartition, 0)
	for _, part := range parts {
		partition, err := strconv.Atoi(part)
		if err == nil {
			tp := TopicPartition{Topic: topic, Partition: partition, Offset: offset, MaxSize: maxSize}
			partitions = append(partitions, &tp)
		}
	}
	return partitions
}

type Broker struct {
	topics      []*TopicPartition
	hostname    string
	Partitioner Partitioner
}

func newBroker(hostname string, tp *TopicPartition) *Broker {

	b := Broker{topics: []*TopicPartition{tp}, hostname: hostname}

	b.Partitioner = func(b *Broker) int {
		return tp.Partition
	}
	return &b

}

func newMultiBroker(hostname string, tplist []*TopicPartition) *Broker {

	b := Broker{topics: tplist, hostname: hostname}
	partitions := make([]int, len(tplist))
	for tpct, tp := range tplist {
		partitions[tpct] = tp.Partition
	}

	b.Partitioner = MakeRandomPartitioner(partitions)
	return &b

}

// creates a broker that uses random paritioner, for a single topic but many partitions
func NewRandomPartitionedBroker(hostname string, topic string, partitions []int) *Broker {
	tplist := make([]*TopicPartition, 0)
	for _, partition := range partitions {
		tp := TopicPartition{Topic: topic, Partition: partition}
		tplist = append(tplist, &tp)
	}
	b := Broker{hostname: hostname, topics: tplist}
	b.Partitioner = MakeRandomPartitioner(partitions)
	return &b
}

// Create a Random Partitioner Func 
func MakeRandomPartitioner(partitions []int) Partitioner {
	rp := rand.New(rand.NewSource(time.Now().UnixNano()))
	partitionSize := len(partitions)
	return func(b *Broker) int {
		return partitions[rp.Intn(partitionSize)]
	}
}

func (b *Broker) connect() (conn *net.TCPConn, er error) {
	raddr, err := net.ResolveTCPAddr(NETWORK, b.hostname)
	if err != nil {
		log.Println("Fatal Error: ", err)
		return nil, err
	}
	conn, err = net.DialTCP(NETWORK, nil, raddr)
	if err != nil {
		log.Println("Fatal Error: ", err)
		return nil, err
	}
	return conn, er
}

// returns buffer reader for single requests
func (b *Broker) readResponse(conn *net.TCPConn) *ByteBuffer {
	reader := bufio.NewReader(conn)
	br := NewByteBuffer(1, reader)
	return br

}

// returns buffer reader for multiple fetch requests (offsets/fetchmsgs)
func (b *Broker) readMultiResponse(conn *net.TCPConn) *ByteBuffer {
	reader := bufio.NewReader(conn)
	br := NewByteBuffer(len(b.topics), reader)
	return br
}
