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
  "encoding/binary"
  "errors"
  "fmt"
  "io"
  "log"
)
/*
Format of a Multi-Fetch RESPONSE

[0 0 9 168 0 0  0 0 6 119  0 0  0 0 0 41  1 1  106 31 14 40 31 139 8 0 0 0 0 0]

0 0 9 168         <REQUEST_SIZE: uint32>
0 0               <ERROR_CODE: uint16>
  -- repeat
  0 0 6 119         <MESSAGE SET SIZE: uint32>
  0 0               <ERROR_CODE: uint16>
    - repeat
    0 0 0 41         <MESSAGE SIZE: uint32>
    1                <magic>
    1                <compression>
    106 31 14 40      <checksum>
    31 139 8 0 0 .... <payload>
*/


// a Byte buffer is a wrapper over bufio.Reader that allows us to forward only read
// while understanding the kafka format for bytes/lenghts, etc
type ByteBuffer struct {
   ct        int
   reader    *bufio.Reader 
   Size      uint32
   consumed  uint32
   //msgs   []*Message
}
func NewByteBuffer(ct int, buf *bufio.Reader) *ByteBuffer {
  b := ByteBuffer{ct: ct, reader:buf}
  return &b
}

func (b *ByteBuffer) Len() int {
  return b.ct
}

// a pattern is uint32, followed by uint16
func (b *ByteBuffer) firstRead() (uint32, uint16, error) {
  
  var err error
  //if b.reader.Buffered() > 30 {
  //  log.Println(b.reader.Peek(30))
  //}
  length := make([]byte, 4)
  lenRead, err := io.ReadFull(b.reader, length)
  if err != nil {
    log.Println("invalid socket read ", err)
    return 0, 0, err
  }
  if lenRead != 4 || lenRead < 0 {
    return 0, 0, errors.New("invalid length of the packet length field")
  }

  expectedLength := binary.BigEndian.Uint32(length)

  shortBytes := make([]byte, 2)
  lenRead, err = io.ReadFull(b.reader, shortBytes)

  if err != nil {
    return 0, 0, err
  }
  if lenRead != 2 || lenRead < 0 {
    return 0, 0, errors.New("invalid length of the short int field")
  }

  shortInt := binary.BigEndian.Uint16(shortBytes)
  //log.Println(length, shortBytes, expectedLength, shortInt)

  return expectedLength, shortInt, nil

}

// initial read of a multi-set response, this will read total len
//  (first 4 bytes) and # of sets
func (b *ByteBuffer) ReadHeader() error {

  size, errorCode, err := b.firstRead() 
  if err != nil {
    log.Println("invalid socket read ", err)
    return err
  }
  b.consumed = 2
  b.Size = size
  if errorCode != 0 {
    sz := b.reader.Buffered()
    errdump := make([]byte, sz)
    _,_ = io.ReadFull(b.reader, errdump)
    log.Println(string(errdump))
    log.Println("errorCode: ",b.reader.Buffered(), size, errorCode)
    return errors.New(fmt.Sprintf("Broker Response Error: %d", errorCode))
  }
  return nil
}

// Read the length and error for this set (message/offset)
func (b *ByteBuffer) ReadSet() (int, error) {

  //log.Println(b.reader.Peek(30))

  size, errorCode, err := b.firstRead() 
  b.consumed += 6
  if errorCode != 0 || err != nil {
    log.Println("errorCode: ", errorCode)
    return 0, errors.New(fmt.Sprintf("Broker Response Error: %d", errorCode))
  }
  return int(size), nil
  
}


func (b *ByteBuffer) NextMsg(payloadCodecsMap map[byte]PayloadCodec) (int, []*Message, error) {
  
  //log.Println("about to get len", b.consumed, b.Size - b.consumed)
  if b.Size - b.consumed < 10 {
    //log.Println("returning, zero len?")
    return 0,nil, nil
  }
  
  //length, err := b.reader.Peek(4)
  length := make([]byte, 4)
  lenRead, err := io.ReadFull(b.reader, length)
  b.consumed += 4
  //log.Println("after len")
  if err != nil {
    log.Println("invalid socket read ", err)
    return 0,nil, err
  }
  if lenRead != 4 {
    return 0,nil, errors.New("invalid length of the packet length field")
  }

  expectedLength := binary.BigEndian.Uint32(length)
  payload := make([]byte, expectedLength)
  b.consumed += expectedLength
  //log.Println("about to get payload")
  lenRead, err = io.ReadFull(b.reader, payload)
  //log.Println("after payload read", lenRead, err)
  if err != nil {
    return 0,nil, err
  }
  if uint32(lenRead) != expectedLength {
    // this is actually an expected condition, the last message in a message 
    // set can be a partial if maxsize was exceeded
    return 0, nil, nil
  }
  //log.Println(payload, expectedLength)
  payload = append(length, payload...)
  payloadConsumed, msgs := Decode(payload, payloadCodecsMap)
  if msgs == nil || len(msgs) == 0 {
    // this isn't invalid as large messages might contain partial messages 
    return 0, []*Message{}, err
  }
  
  return int(payloadConsumed) , msgs, err
  
}

func (b *ByteBuffer) Payload() ([]byte, error) {
  
  var err error

  length := make([]byte, 4)
  lenRead, err := io.ReadFull(b.reader, length)
  if err != nil {
    log.Println("invalid socket read ", err)
    return []byte{}, err
  }
  if lenRead != 4 || lenRead < 0 {
    return []byte{}, errors.New("invalid length of the packet length field")
  }

  expectedLength := binary.BigEndian.Uint32(length)
  payload := make([]byte, expectedLength)
  lenRead, err = io.ReadFull(b.reader, payload)
  if err != nil {
    return []byte{}, err
  }
  if lenRead != int(expectedLength) {
    return []byte{}, errors.New("invalid length of the packet length field")
  }
  return payload, err
  
}