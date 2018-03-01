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

