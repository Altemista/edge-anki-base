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

var cmds = []Command{
	{
		Command: "c",
		CarNo:   1,
		Param1:  "",
		Param2:  "left",
	},
	{
		Command: "c",
		CarNo:   1,
		Param1:  "",
		Param2:  "left",
	},
	{
		Command: "c",
		CarNo:   1,
		Param1:  "",
		Param2:  "right",
	},
	{
		Command: "c",
		CarNo:   1,
		Param1:  "",
		Param2:  "right",
	},
	{
		Command: "c",
		CarNo:   1,
		Param1:  "",
		Param2:  "wrong",
	},
}

var currentLanes = []int{
	1, 4, 1, 4, 1,
}

var results = []string{
	"2", "4", "1", "3", "",
}

func TestCalculateLaneNo(t *testing.T) {
	t.Log("Testing CalculateLaneNo")
	for i := range cmds {
		cmds[i].CalculateLaneNo(currentLanes[i])
		if cmds[i].Param1 != results[i] {
			t.Errorf("Error in case %d: Expected laneNo >%s< found laneNo >%s<", i, results[i], cmds[i].Param1)
			t.Fail()
		}
	}
}
