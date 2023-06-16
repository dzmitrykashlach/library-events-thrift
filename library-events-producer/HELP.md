### Kafka commands

#### Start kafka:
`./zookeeper-server-start.bat ../../config/zookeeper.properties`
`./kafka-server-start.bat ../../config/server(-1,2).properties`

#### Create topic:
`./kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4`

#### List topics:
`.\kafka-topics.bat --bootstrap-server localhost:9092 --list`

#### Instantiate console producer:
`./kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic`

#### Instantiate console consumer:
`./kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic library-events --from-beginning`

#### curl
`curl -i \
-d '{"libraryEventId":null,"book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X POST http://localhost:8080/v1/libraryevent`