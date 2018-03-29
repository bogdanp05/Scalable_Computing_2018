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

kafka_create:
	@docker pull spotify/kafka
	@docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 --name kafka -h kafka spotify/kafka

kafka_topic:
	@docker exec kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

cluster_master:
	@docker run -d --rm --name spark-master -p 4040:4040 -p 8080-8081:8080-8081 -p 7077:7077 briansetz/docker-spark:2.2.1 spark/sbin/start-master.sh

cluster_slave:
	$(eval MASTER_IP=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master))
	@echo ${MASTER_IP}
	@docker run -d --rm --name spark-slave briansetz/docker-spark:2.2.1 spark/sbin/start-slave.sh spark://${MASTER_IP}:7077

compose:
	@docker-compose up -d --build --remove-orphans

assembly:
	@(cd ./Project && sbt assembly)

deploy:
	$(eval MASTER_IP=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master))
	@echo ${MASTER_IP}
	$(eval MONGO_IP=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongoCluster))
	@echo ${MONGO_IP}
	$(eval KAFKA_IP=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka))
	@echo ${KAFKA_IP}
	${SPARK_HOME}/bin/spark-submit --class nl.rug.sc.app.SparkSubmitMain --deploy-mode client --master spark://${MASTER_IP}:7077 --executor-memory 5G ./Project/target/scala-2.11/Project-assembly-0.1.jar ${MONGO_IP} $(KAFKA_IP)

