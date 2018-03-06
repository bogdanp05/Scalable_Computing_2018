#type "make mongo" in terminal to run mongoDB with docker 
mongodb:
	@docker volume create mongo-vol
	@docker run --name mongoSC -p 27017:27017 -v mongo-vol:/mongoDB -d mongo

#type "make stop" in terminal to stop the container
stop:
	@docker stop mongoSC

start:
	@docker start mongoSC

remove:
	@docker rm mongoSC
	@docker rm mongo-vol

kafka:
	@docker pull spotify/kafka
	@docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 --name kafka -h kafka spotify/kafka
	@docker exec kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test


