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
	"flag"
	"fmt"
	"log"
	"os"
	//"os/signal"
	kafka "github.com/apache/kafka/clients/go/src"
	"strconv"
	"strings"
	"time"
	//"syscall"
)

var hostname string
var topic string
var partitionstr string
var offset uint64
var maxSize uint
var writePayloadsTo string
var consumerForever bool
var printmessage bool

func init() {
	flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
	flag.StringVar(&topic, "topic", "test", "topic to publish to")
	flag.StringVar(&partitionstr, "partitions", "0", "partitions to publish to:  comma delimited")
	flag.Uint64Var(&offset, "offset", 0, "offset to start consuming from")
	flag.UintVar(&maxSize, "maxsize", 1048576, "max size in bytes to consume a message set")
	flag.StringVar(&writePayloadsTo, "writeto", "", "write payloads to this file")
	flag.BoolVar(&consumerForever, "consumeforever", true, "loop forever consuming")
	flag.BoolVar(&printmessage, "print", true, "print the message details to stdout")
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func main() {
	flag.Parse()
	fmt.Println("Consuming Messages :")
	fmt.Printf("From: %s, topic: %s, partitions: %s\n", hostname, topic, partitionstr)
	log.Println("printing ?", printmessage)
	//panic("wt")
	fmt.Println(" ---------------------- ")
	var broker *kafka.BrokerConsumer

	parts := strings.Split(partitionstr, ",")
	if len(parts) > 1 {
		tps := kafka.NewTopicPartitions(topic, partitionstr, offset, uint32(maxSize))
		broker = kafka.NewMultiConsumer(hostname, tps)
	} else {
		partition, _ := strconv.Atoi(partitionstr)
		broker = kafka.NewBrokerConsumer(hostname, topic, partition, offset, uint32(maxSize))
	}

	var payloadFile *os.File = nil
	var msgCt int
	if len(writePayloadsTo) > 0 {
		var err error
		payloadFile, err = os.Create(writePayloadsTo)
		if err != nil {
			fmt.Println("Error opening file: ", err)
			payloadFile = nil
		}
	}
	var consumerCallback kafka.MessageHandlerFunc
	consumerCallback = func(topic string, partition int, msg *kafka.Message) {
		msgCt++
		if printmessage {
			msg.Print()
		} else if msgCt == 1000 {
			fmt.Printf("Cur Offset: %d\n", msg.Offset()+msg.TotalLen())
			msgCt = 0
		}
		if msgCt == 10 {
			//panic("out")
		}
		if payloadFile != nil {
			payloadFile.Write([]byte("Message at: " + strconv.FormatUint(msg.Offset(), 10) + "\n"))
			payloadFile.Write(msg.Payload())
			payloadFile.Write([]byte("\n-------------------------------\n"))
		}
	}

	if consumerForever {

		//quit := make(chan bool, 1)
		done := make(chan bool, 1)
		msgChan := make(chan *kafka.Message)
		/*
		   go func() {
		     var sigIn chan os.Signal = make(chan os.Signal,1)
		     signal.Notify(sigIn)
		     for {

		       select {
		       case <-quit:
		         log.Println("got quit, shutting down")
		         close(msgChan)
		         return
		       case sig := <-sigIn:
		         if sig.(os.Signal) == syscall.SIGINT {
		           done <- true
		         } else {
		           fmt.Println(sig)
		         }
		       }
		     }
		   }()
		*/
		// lets try restarting this every 1 minute to see if it works?
		timer := time.NewTicker(time.Second * 20)
		runConsumer := func(donech chan bool, mch chan *kafka.Message) {
			partition, _ := strconv.Atoi(partitionstr)
			log.Println(hostname, topic, partition, offset, uint32(maxSize))
			brok := kafka.NewBrokerConsumer(hostname, topic, partition, offset, uint32(maxSize))
			go brok.ConsumeOnChannel(mch, 1000, donech)
			for msg := range mch {
				if msg != nil {
					consumerCallback(topic, 0, msg)
				} else {
					break
				}
			}
		}
		//go func() {
		go runConsumer(done, msgChan)
		for _ = range timer.C {
			// old done channel? 
			log.Println("about to send done signal!")
			done <- true
			log.Println("after done signal")
			time.Sleep(time.Second * 2)
			done = make(chan bool, 1)
			mch := make(chan *kafka.Message)
			log.Println("restarting")
			runConsumer(done, mch)
		}
		//}()

	} else {
		broker.Consume(consumerCallback)
	}

	if payloadFile != nil {
		payloadFile.Close()
	}

}
