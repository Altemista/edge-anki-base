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

type (
	// Status represents a status update message from the Anki Overdrive controller
	// Valid examples are:
	//  { "status_id" : "23", "status_name" : "Ping" }
	//  { "status_id" : "25", "status_name" : "Version", "version" : 42 }
	//  { ... }
	Status struct {
		StatusID      string  `json:"status_id"`
		StatusName    string  `json:"status_name"`
		CarNo         string  `json:"car_no"`
		CarID         string  `json:"car_id"`
		Version       int     `json:"version"`
		Level         int     `json:"level"`
		Offset        float32 `json:"offset"`
		Speed         int     `json:"speed"`
		PieceID       int     `json:"piece_id"`
		PieceLocation int     `json:"piece_location"`
	}
)

// MergeStatusUpdate updates fields as per message type
func (s *Status) MergeStatusUpdate(u *Status) {
	if u.StatusID == "23" {
		// No update, just a ping
	} else if u.StatusID == "25" {
		// Version update
		s.Version = u.Version
	} else if u.StatusID == "27" {
		// Battery update
		s.Level = u.Level
	} else if u.StatusID == "39" {
		// Position update
		s.PieceID = u.PieceID
		s.PieceLocation = u.PieceLocation
		s.Speed = u.Speed
		s.Offset = u.Offset
	} else if u.StatusID == "41" {
		// Transition update
		s.Offset = u.Offset
	} else if u.StatusID == "43" {
		// Delocalisation, not sure what to do
	} else if u.StatusID == "45" {
		// Offset update
		s.Offset = u.Offset
	} else if u.StatusID == "65" {
		// Offset update (undocumented?)
		s.Offset = u.Offset
	}
}
