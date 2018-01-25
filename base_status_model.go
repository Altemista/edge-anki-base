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
	"fmt"
	"time"
	"strings"
	"strconv"
)

type (
	// Status represents a status update message from the Anki Overdrive controller
	Status struct {
		MsgID               int         `json:"msgID"`
		MsgName             string      `json:"msgName"`
		MsgTimestamp        time.Time   `json:"msgTimestamp"`
		CarNo               int         `json:"carNo"`
		CarID               string      `json:"carID"`
		CarSpeed            int         `json:"carSpeed"`
		CarVersion          int         `json:"carVersion"`
		CarBatteryLevel     int         `json:"carBatteryLevel"`
		LaneOffset          int     	`json:"laneOffset"`
		LaneNo              int         `json:"laneNo"`
		LaneLength          int         `json:"laneLength"`
		LaneTimestamp       time.Time   `json:"laneTimestamp"`
		PosTileType         string      `json:"posTileType"`
		PosTileNo           int         `json:"posTileNo"`
		PosLocation         int         `json:"posLocation"`
		PosTimestamp        time.Time   `json:"posTimestamp"`
		PosOptions          []PosOption `json:"posOptions"`
		MaxTileNo           int         `json:"maxTileNo"`
		TransitionTimestamp time.Time
		LightsOn			bool
	}
	// PosOption lists an option for a position
	PosOption struct {
		OptTileNo      int `json:"optTileNo"`
		OptProbability int `json:"optProbability"`
	}
)

// Identify returns a semi.unique ID
func (s Status) Identify() string {
	return fmt.Sprintf("%v", s.MsgTimestamp.UnixNano())
}

// MergeStatusUpdate updates fields as per message type 2
func (s *Status) MergeStatusUpdate(u Status) {
	defer Track_execution_time(Start_execution_time("MergeStatusUpdate"))

	if u.MsgID == 23 {
		// No update, just a ping
	} else if u.MsgID == 25 {
		// Version update
		s.CarVersion = u.CarVersion
		s.MsgTimestamp = u.MsgTimestamp
	} else if u.MsgID == 27 {
		// Battery update
		s.CarBatteryLevel = u.CarBatteryLevel
		s.MsgTimestamp = u.MsgTimestamp
	} else if u.MsgID == 39 {
		// Position update
		s.CarID = u.CarID
		s.CarSpeed = u.CarSpeed
		s.LaneOffset = u.LaneOffset
		s.LaneNo = u.LaneNo
		s.LaneLength = u.LaneLength
		s.PosTileType = u.PosTileType
		s.PosLocation = u.PosLocation
		s.PosOptions = u.PosOptions
		s.PosTimestamp = u.MsgTimestamp
		s.MsgTimestamp = u.MsgTimestamp
		s.MaxTileNo = u.MaxTileNo
		s.findTileNo(u)
	} else if u.MsgID == 41 {
		// Transition update
		s.CarSpeed = u.CarSpeed
		s.LaneOffset = u.LaneOffset
		s.LaneNo = u.LaneNo
		s.LaneLength = u.LaneLength
		s.PosTileType = u.PosTileType
		s.PosLocation = u.PosLocation
		s.PosOptions = u.PosOptions
		s.PosTimestamp = u.MsgTimestamp
		s.MsgTimestamp = u.MsgTimestamp
		s.MaxTileNo = u.MaxTileNo
		s.TransitionTimestamp = u.MsgTimestamp
		s.findTileNo(u)
	} else if u.MsgID == 43 {
		// Delocalisation, not sure what to do
	} else if u.MsgID == 45 {
		// Offset update
		s.LaneOffset = u.LaneOffset
		s.MsgTimestamp = u.MsgTimestamp
	} else if u.MsgID == 65 {
		// Offset update (undocumented?)
		s.LaneOffset = u.LaneOffset
		s.MsgTimestamp = u.MsgTimestamp
	}
}

// MergeStatusUpdate updates fields as per message type
func (s *Status) findTileNo(u Status) {
	bestTileNo := -1
	bestProbability := 0
	for _, option := range u.PosOptions {
		if option.OptProbability > bestProbability {
			bestTileNo = option.OptTileNo
			bestProbability = option.OptProbability
		}
	}
	s.PosTileNo = bestTileNo
}

func parseCSV(csv string) (Status, error) {
	defer Track_execution_time(Start_execution_time("parseCSV"))

	status := Status{}
	var err error

	splits := strings.Split(csv, ";")
	status.MsgID, err = strconv.Atoi(splits[0])
	if err != nil {
		return status, err
	}

	status.MsgTimestamp, err = time.Parse(time.RFC3339, splits[1])
	if err != nil {
		return status, err
	}

	status.CarNo, err = strconv.Atoi(splits[2])
	if err != nil {
		return status, err
	}

	status.PosLocation, err = strconv.Atoi(splits[3])
	if err != nil {
		return status, err
	}

	status.PosTileNo, err = strconv.Atoi(splits[4])
	if err != nil {
		return status, err
	}

	status.CarSpeed, err = strconv.Atoi(splits[5])
	if err != nil {
		return status, err
	}

	status.LaneNo, err = strconv.Atoi(splits[6])
	status.LaneLength, err = strconv.Atoi(splits[7])
	status.MaxTileNo, err = strconv.Atoi(splits[8])

	posOpts := strings.Split(splits[9], ":")

	for _, posOptString := range posOpts {
		posOptSplit := strings.Split(posOptString, ",")
		if(len(posOptSplit) < 2) {
			continue
		}

		posOpt := PosOption{ }

		posOpt.OptProbability, err = strconv.Atoi(posOptSplit[0])
		if err != nil {
			continue
		}

		posOpt.OptTileNo, err = strconv.Atoi(posOptSplit[1])
		if err != nil {
			continue
		}

		status.PosOptions = append(status.PosOptions, posOpt)
	}

	return status, err
}
