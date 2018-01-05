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
)

var m_statusCh chan Status

func CreateHttpConsumer(statusCh chan Status) (error) {
	plog.Println("Starting http listener")

	m_statusCh = statusCh

	http.HandleFunc("/status", http_status_handler)
	go http.ListenAndServe(":8089", nil)

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
