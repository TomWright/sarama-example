# sarama-example

Start kafka with:
```bash
docker-compose --project-name=sarama-example up
```

Open 3 new terminal windows and run the following in each:
```
KAFKA_CONSUMER_ADDRESS=localhost:9092 KAFKA_PRODUCER_ADDRESS=localhost:9092 go run main.go
```

Every 20 seconds, each instance of the app will publish 5 messages.

You should see that only 1 of the apps will be receiving any messages.

Even stopping the runner that is receiving the messages to cause a rebalance just pushes all messages to one of the other consumers.

Run the following to clean up kafka
```
docker-compose --project-name=sarama-example down

docker-compose --project-name=sarama-example rm
```
