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
)

// Variable plog is the logger for the package
var plog = log.New(os.Stdout, "EDGE-ANKI-BASE: ", log.Lshortfile|log.LstdFlags)

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
	cmdCh := make(chan Command)
	go sendCommand(cmdCh)

	// Consumer
	statusCh := make(chan Status)
	err := CreateHttpConsumer(statusCh)
	if err != nil {
		return nil, nil, err
	}
	return cmdCh, statusCh, nil
}

func sendCommand(ch chan Command) {
	var cmd Command
	for {
		plog.Printf("INFO: Waiting for command at %v", time.Now())
		cmd = <-ch
		plog.Printf("INFO: Received command")
		cmdstr, err := cmd.ControllerString()
		plog.Printf("INFO: Sending command %s to topic %s", cmdstr, "Command" + strconv.Itoa(cmd.CarNo))
		if err != nil {
			plog.Println("WARNING: Ignoring command due to decoding error")
			continue
		}

		requestUrl := "http://localhost:809" + strconv.Itoa(cmd.CarNo) + "/cmd"
		var netClient = &http.Client{
			Timeout: time.Second * 10,
		}
		response, err := netClient.Post(requestUrl, "text/plain", bytes.NewBuffer([]byte(cmdstr)))
		if err != nil {
			plog.Println("WARNING: Could not send command")
		}
		defer response.Body.Close()
	}
}
