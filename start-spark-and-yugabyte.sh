#!/bin/bash

docker build -f data_lakehouse/Dockerfile.Spark . -t spark-air

docker-compose -f data_lakehouse/docker-compose.Lakehouse.yaml -f yugabytesDB/docker-compose.Yugabyte.yaml up -d

docker cp data_lakehouse/restore.sql yb-tserver-n1:/home/yugabyte/ 

# docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1 \i restore.sql

# docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1 -U postgres -d dvdrental \dt

docker exec -it master bash /opt/workspace/dependencies/packages_installer.sh 

docker exec -it master spark-submit --master spark://master:7077 /opt/bitnami/spark/bronze_data_to_s3.py
docker exec -it master spark-submit --master spark://master:7077 /opt/bitnami/spark/silver_data.py