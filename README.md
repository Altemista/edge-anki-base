# Base Library for Anki Overdrive
This includes the generic code with which the Anki Use Cases can communicate
with the Anki Controller through Kafka. For the application there are no
depedencies on Kafka as all communication is through Go channels.

The generic can be used as follows:

First import the base package (consider using `glide` for vendoring).
```
import (
	...
	anki "github.com/okoeth/edge-anki-base"
	...
)
```

Set-up the variables for track and statrus and command channels:
```
// Set-up channels for status and commands
cmdCh, statusCh, err := anki.CreateChannels("edge.overtake")
if err != nil {
	mlog.Fatalln("FATAL: Could not establish channels: %s", err)
}
track := anki.CreateTrack()

// Go and watch the track
go watchTrack(track, cmdCh, statusCh)
```

