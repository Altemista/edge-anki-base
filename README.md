# Base Library for Anki Overdrive
This includes the generic code with which the Anki Use Cases can communicate
with the Anki Controller through Kafka.

The generic can be used as follows:

First import the base package (consider using `glide` for vendoring).
```
import (
	...
	"github.com/okoeth/edge-anki-base/anki"
	...
)
```

Set-up the variables for Kafka producer and consumer. `TheStatus` is the array which holds the status
for the three cars.
```
// TheStatus carries the latest status information
var TheStatus = [3]anki.Status{}

// TheProducer provides a reference to the Kafka producer
var TheProducer sarama.AsyncProducer

// TheConsumer provides a reference to the Kafka producer
var TheConsumer *consumergroup.ConsumerGroup
```

Then in your main function initialise the variables:
``` 
// Set-up Kafka
kafkaServer := os.Getenv("KAFKA_SERVER")
if kafkaServer == "" {
	mlog.Printf("INFO: Using 127.0.0.1 as default KAFKA_SERVER.")
	kafkaServer = "127.0.0.1"
}
p, err := anki.CreateKafkaProducer(kafkaServer + ":9092")
if err != nil {
	mlog.Fatalf("ERROR: Cannot create Kafka producer: %s", err)
}
TheProducer = p
c, err := anki.CreateKafkaConsumer(kafkaServer+":2181", TheStatus)
if err != nil {
	mlog.Fatalf("ERROR: Cannot create Kafka consumer: %s", err)
}
TheConsumer = c
```


