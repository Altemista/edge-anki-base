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

import "testing"
import "encoding/json"

var dataMarshaling = [][]byte{[]byte(`{ 
		"msgID":39,
		"msgName":"POSITION_UPDATE",
		"msgTimestamp":"2017-12-02T21:04:20.071Z",
		"carNo":1,
		"carID":"asdf",
		"carSpeed":258,
		"posTileType":"17",
		"posLocation":33,
		"posTileNo":1,
		"posOptions": [
			{
				"optTileNo":1,
				"optProbability":90
			},
			{
				"optTileNo":4,
				"optProbability":10
			}
		],
		"laneNo":1,
		"laneOffset":-67.44000244140625
	}`),
	[]byte(`{ 
		"msgID":39,
		"msgName":"POSITION_UPDATE",
		"msgTimestamp":"2017-12-02T21:04:20.071Z",
		"carNo":1,
		"carID":"asdf",
		"carSpeed":258,
		"posTileType":"17",
		"posLocation":33,
		"posTileNo":1,
		"laneNo":1,
		"laneOffset":-67.44000244140625
	}`)}

var tss = []Status{
	Status{
		MsgID: 39,
		PosOptions: []PosOption{
			{
				OptTileNo:      1,
				OptProbability: 90,
			},
			{
				OptTileNo:      4,
				OptProbability: 10,
			},
		},
	},
	Status{
		MsgID:      39,
		PosOptions: []PosOption{},
	},
}

var dataTileNo = []Status{
	Status{
		MsgID: 39,
		PosOptions: []PosOption{
			{
				OptTileNo:      1,
				OptProbability: 90,
			},
			{
				OptTileNo:      4,
				OptProbability: 10,
			},
		},
	},
	Status{
		MsgID: 39,
		PosOptions: []PosOption{
			{
				OptTileNo:      1,
				OptProbability: 10,
			},
			{
				OptTileNo:      4,
				OptProbability: 90,
			},
		},
	},
	Status{
		MsgID: 39,
		PosOptions: []PosOption{
			{
				OptTileNo:      1,
				OptProbability: 100,
			},
		},
	},
	Status{
		MsgID:      39,
		PosOptions: []PosOption{},
	},
}

var resultTileNo = []int{
	1, 4, 1, -1,
}

func TestMarshaling(t *testing.T) {
	t.Log("Testing marshaling")
	for i := range dataMarshaling {
		s := Status{}
		err := json.Unmarshal(dataMarshaling[i], &s)
		if err != nil {
			t.Errorf("Error unmarishalling: %v", err)
			t.Fail()
		}
		if s.MsgID != tss[i].MsgID {
			t.Errorf("MessageID: %d", s.MsgID)
			t.Fail()
		}
		if len(s.PosOptions) != len(tss[i].PosOptions) {
			t.Errorf("Number of elements: %d", len(s.PosOptions))
			t.Fail()
		}
		if s.Identify() == "" {
			t.Errorf("Empty identifier: %s", s.Identify())
			t.Fail()
		}
		//TODO: More field tests to add
	}
}

func TestTileNo(t *testing.T) {
	t.Log("Testing tile-no")
	currentStatus := Status{}
	for i := range dataTileNo {
		currentStatus.findTileNo(dataTileNo[i])
		if currentStatus.PosTileNo != resultTileNo[i] {
			t.Errorf("Wrong tileNo, expcted: %d / found: %d", resultTileNo[i], currentStatus.PosTileNo)
			t.Fail()
		}
	}
}
