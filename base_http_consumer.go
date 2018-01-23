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
	"net/http"
	"encoding/json"
	"io/ioutil"
	"github.com/gorilla/websocket"
	"goji.io"
	"goji.io/pat"
	"time"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize: 1024,
	WriteBufferSize: 1024,
}

var m_statusCh chan Status
var m_track* []Status

func CreateHttpConsumer(statusCh chan Status, mux* goji.Mux, track* []Status) (error) {
	plog.Println("Starting http listener")

	m_statusCh = statusCh
	m_track = track

	//http.HandleFunc("/status", websocket_handler)
	mux.HandleFunc(pat.Get("/status"), websocket_handler)

	return nil
}

func CreateAdasHttpConsumer(statusCh chan Status, mux* goji.Mux, track* []Status) (error) {
	plog.Println("Starting http listener")

	m_statusCh = statusCh
	m_track = track

	//http.HandleFunc("/status", websocket_handler)
	mux.HandleFunc(pat.Get("/status"), websocket_adas_handler)

	return nil
}

func http_status_handler(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		plog.Printf("WARNING: Could not read body: %s", req.Body)
	}
	plog.Println("INFO: Received message: " + string(body))

	update := Status{}
	err = json.Unmarshal(body, &update)
	if err != nil {
		plog.Printf("WARNING: Could not unmarshal message, ignoring: %s", body)
	}
	m_statusCh <- update
}

func websocket_handler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		plog.Println(err)
		return
	}
	plog.Println("Client subscribed")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			plog.Println(err)
			return
		}

		start := time.Now()
		plog.Println("INFO: Received message")

		update := Status{}
		err = json.Unmarshal(msg, &update)
		if err != nil {
			plog.Printf("WARNING: Could not unmarshal message, ignoring: %s", string(msg))
		}
		plog.Printf("INFO: Unmarshalling message took %f ms", time.Since(start).Seconds()*1000)
		m_statusCh <- update
	}
	plog.Println("Client unsubscribed")
}

func websocket_adas_handler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		plog.Println(err)
		return
	}
	plog.Println("Client subscribed")

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			plog.Println(err)
			return
		}

		start := time.Now()
		plog.Println("INFO: Received message: " + string(msg))
		update := Status{}
		update, err = parseCSV(string(msg))
		if err != nil {
			plog.Printf("WARNING: Could not unmarshal message, ignoring: %s", msg)
		}
		plog.Printf("INFO: Unmarshalling message took %f ms", time.Since(start).Seconds()*1000)
		UpdateTrack(*m_track, update)
	}
	plog.Println("Client unsubscribed")
}
