# 2018_group_6_s3480941_s3346935_s3421902

- Get the dataset from https://labrosa.ee.columbia.edu/millionsong/tasteprofile. It will be a 1.8GB textfile.
- Use txt_to_csv.py to convert it to csv.
- Add the csv file in Project/src/main/resources/csv.
- Use "make mongodb" to start MongoDB.
- Use csv_to_mongo.py to populate the mongo database with the csv.

__
- Use "make kafka_create" to set up a kafka instance.
- Use "make kafka_topic" to create the topic "test".
- In `/etc/hosts` on your pc, map "kafka" to the ip address of the docker continer (e.g. `172.17.0.3 kafka`)



