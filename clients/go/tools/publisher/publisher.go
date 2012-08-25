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

package main

import (
	"bufio"
	"flag"
	"fmt"
	kafka "github.com/apache/kafka/clients/go/src"
	"os"
)

/*
 This publisher tool has 4 send modes:
 1.  Pass message:   
         ./publisher -message="good stuff bob" -hostname=192.168.1.15:9092

 2.  Pass Msg, SendCT:  Send the samge message SendCt # of times 
        ./publisher -sendct=100 -message="good stuff bob"

 3.  MessageFile:  pass a message file and it will read 
          ./publisher -messagefile=/tmp/msgs.log

 4.  Stdin:  if message, message file empty it accepts 
              messages from Console (message end at new line)
              ./publisher -topic=atopic -partition=0
               >my message here<enter>

*/
var hostname string
var topic string
var partition int
var sendCt int
var message string
var messageFile string
var compress bool

func init() {
	flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
	flag.StringVar(&topic, "topic", "test", "topic to publish to")
	flag.IntVar(&partition, "partition", 0, "partition to publish to")
	flag.StringVar(&message, "message", "", "message to publish")
	flag.IntVar(&sendCt, "sendct", 0, "to do a pseudo load test, set sendct & pass a message ")
	flag.StringVar(&messageFile, "messagefile", "", "read message from this file")
	flag.BoolVar(&compress, "compress", false, "compress the messages published")
}

// sends 
func SendFile(msgFile string) {
	broker := kafka.NewBrokerPublisher(hostname, topic, partition)
	fmt.Println("Publishing File:", msgFile)
	file, err := os.Open(msgFile)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	stat, err := file.Stat()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	payload := make([]byte, stat.Size())
	file.Read(payload)
	timing := kafka.StartTiming("Sending")

	if compress {
		broker.Publish(kafka.NewCompressedMessage(payload))
	} else {
		broker.Publish(kafka.NewMessage(payload))
	}

	timing.Print()
	file.Close()
}

func SendMessage() {

	broker := kafka.NewBrokerPublisher(hostname, topic, partition)

	fmt.Println("Publishing :", message)
	if compress {
		broker.Publish(kafka.NewCompressedMessage([]byte(message)))
	} else {
		broker.Publish(kafka.NewMessage([]byte(message)))
	}
}

func SendManyMessages() {

	broker := kafka.NewBrokerPublisher(hostname, topic, partition)
	timing := kafka.StartTiming("Sending")

	fmt.Println("Publishing :", message, ": Will send ", sendCt, " times")
	done := make(chan bool)
	msgChan := make(chan *kafka.Message, 1000)

	go broker.PublishOnChannel(msgChan, 100, 100, done)
	for i := 0; i < sendCt; i++ {
		msgChan <- kafka.NewMessage([]byte(message))
	}
	done <- true // force flush

	timing.Print()
}

func main() {
	flag.Parse()
	fmt.Printf("Kafka: %s, topic: %s, partition: %d\n", hostname, topic, partition)
	fmt.Println(" ---------------------- ")

	if len(message) == 0 && len(messageFile) != 0 {

		SendFile(messageFile)

	} else if len(message) > 0 && sendCt == 0 {

		SendMessage()

	} else if len(message) > 0 && sendCt > 0 {

		SendManyMessages()

	} else {

		// console publisher
		broker := kafka.NewBrokerPublisher(hostname, topic, partition)
		b := bufio.NewReader(os.Stdin)
		done := make(chan bool)
		msgChan := make(chan *kafka.Message, 1000)

		go broker.PublishOnChannel(msgChan, 2000, 200, done)
		fmt.Println("reading from stdin")
		for {
			if s, e := b.ReadString('\n'); e == nil {

				fmt.Println("sending ---", s)

				msgChan <- kafka.NewMessage([]byte(s))

			}
		}
	}
}
