// Copyright 2018 NTT Group

// Permission is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in the Software
// without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to the following
// conditions:

// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
// PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

package anki

import (
	"log"
	"os"
	"strconv"
	"time"
	"net/http"
	"bytes"
	"goji.io"
	"sync"
	"github.com/Shopify/sarama"
)

var lock sync.Mutex

// Variable plog is the logger for the package
var plog = log.New(os.Stdout, "EDGE-ANKI-BASE: ", log.Lshortfile|log.LstdFlags|log.Lmicroseconds)

// SetLogger sets-up the
func SetLogger(l *log.Logger) {
	plog = l
}

// CreateTrack sets-up the
func CreateTrack() []Status {
	track := [6]Status{}
	for i := 0; i < 4; i++ {
		track[i].CarNo = i
	}
	track[4].CarNo = -1
	track[5].CarNo = -2
	return track[:]
}

// UpdateTrack merges a new status update in the track
func UpdateTrack(track []Status, update Status) {
	defer Track_execution_time(Start_execution_time("UpdateTrack"))

	lockTime := time.Now()
	lock.Lock()
	defer lock.Unlock()
	plog.Printf("INFO: ======= Waited at UpdateTrack lock for %f ms =======", time.Since(lockTime).Seconds()*1000)

	plog.Printf("INFO: Updating track from status update with latency %f ms", time.Since(update.MsgTimestamp).Seconds()*1000)
	if update.CarNo == 0 {
		track[0].MergeStatusUpdate(update)
	} else if update.CarNo == 1 {
		track[1].MergeStatusUpdate(update)
	} else if update.CarNo == 2 {
		track[2].MergeStatusUpdate(update)
	} else if update.CarNo == 3 {
		track[3].MergeStatusUpdate(update)
	} else if update.CarNo == -1 {
		track[4].MergeStatusUpdate(update)
	} else if update.CarNo == -2 {
		track[5].MergeStatusUpdate(update)
	} else {
		plog.Printf("WARNING: Ignoring message from unknown carNo: %d", update.CarNo)
	}
}

// CreateChannels Set-up of Communication (hiding all Kafka details behind Go Channels)
func CreateChannels(uc string) (chan Command, chan Status, error) {
	// Set-up Kafka
	kafkaServer := os.Getenv("KAFKA_SERVER")
	if kafkaServer == "" {
		plog.Printf("INFO: Using 127.0.0.1 as default KAFKA_SERVER.")
		kafkaServer = "127.0.0.1"
	}
	// Producer
	p, err := CreateKafkaProducer(kafkaServer + ":9092")
	if err != nil {
		return nil, nil, err
	}
	cmdCh := make(chan Command)
	go sendCommand(p, cmdCh)
	// Consumer
	statusCh := make(chan Status)
	_, err = CreateKafkaConsumer(kafkaServer+":2181", uc, statusCh)
	if err != nil {
		return nil, nil, err
	}
	return cmdCh, statusCh, nil
}

func sendCommand(p sarama.AsyncProducer, ch chan Command) {
	var cmd Command
	for {
		plog.Printf("INFO: Waiting for command at %v", time.Now())
		cmd = <-ch
		plog.Printf("INFO: Received command")
		cmdstr, err := cmd.ControllerString()
		if err != nil {
			plog.Println("WARNING: Ignoring command due to decoding error")
			continue
		}
		p.Input() <- &sarama.ProducerMessage{
			Value:     sarama.StringEncoder(cmdstr),
			Topic:     "Command" + strconv.Itoa(cmd.CarNo),
			Partition: 0,
			Timestamp: time.Now(),
		}

	}
}


// CreateChannels Set-up of Communication (hiding all Kafka details behind Go Channels)
func CreateHttpChannels(uc string, mux* goji.Mux, track* []Status) (chan Command, chan Status, error) {
	// Producer
	cmdCh := make(chan Command)
	go sendHttpCommand(cmdCh)

	// Consumer
	statusCh := make(chan Status)
	if uc == "edge.adas" {
		err := CreateAdasHttpConsumer(statusCh, mux, track)
		if err != nil {
			return nil, nil, err
		}
	} else {
		err := CreateHttpConsumer(statusCh, mux, track)
		if err != nil {
			return nil, nil, err
		}
	}
	return cmdCh, statusCh, nil
}

func sendHttpCommand(ch chan Command) {
	var cmd Command
	for {
		plog.Printf("INFO: Waiting for command at %v", time.Now())
		cmd = <-ch
		plog.Printf("INFO: Received command")
		cmdstr, err := cmd.ControllerString()

		carServer := os.Getenv("CAR_HTTP_SERVER")
		if carServer == "" {
			plog.Printf("INFO: Using localhost as default CAR_HTTP_SERVER.")
			carServer = "localhost"
		} else {
			plog.Printf("INFO: Using " + carServer + " as CAR_HTTP_SERVER.")
		}
		requestUrl := "http://" + carServer + ":809" + strconv.Itoa(cmd.CarNo) + "/cmd"

		plog.Printf("INFO: Sending command %s to address %s", cmdstr, "Command" + requestUrl)
		if err != nil {
			plog.Println("WARNING: Ignoring command due to decoding error")
			continue
		}

		var netClient = &http.Client{
			Timeout: time.Second * 10,
		}
		response, err := netClient.Post(requestUrl, "text/plain", bytes.NewBuffer([]byte(cmdstr)))
		if err != nil {
			plog.Println("WARNING: Could not send command")
		}

		if response != nil {
			defer response.Body.Close()
		}
	}
}
