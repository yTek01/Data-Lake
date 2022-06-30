


* Navigate to the ./data_lakehouse directory and execute the command below.

Taging for Spark 
```BASH
docker build -f data_lakehouse/Dockerfile.Spark . -t cluster-apache-spark:3.1.1
```


* Now start the Lakehouse containers
```BASH
docker-compose -f docker-compose.Lakehouse.yaml up -d
```

```BASH
docker exec -it master bash /opt/workspace/dependencies/packages_installer.sh 
```

```
docker exec -it master spark-submit --master spark://master:7077 /opt/workspace/postgres_to_s3.py
```






* Go to https://9001-devilisdefe-deltalakeet-1na534igs3o.ws-eu47.gitpod.io/login to check that MinIO is running. Login is provided below.

```BASH
USER=admin
PASSWORD=123456789
```

* Create a Postgres database, we are going to name it CarParts and use CarParts.sql file to create tables).
```BASH
docker exec -it postgres /bin/sh
psql -U root -d dvdrental -W
password: root
```

psql -U postgres -d dvdrental -W


Use the SQL commands in CarParts.sql file to create the tables and `\dt` to confirm the list. 

docker exec -it master bash /opt/workspace/dependencies/packages_installer.sh 


* Create an S3 Bucket
```

```

* Run Spark Jobs from the Docker containers.
```BASH
docker exec -it master spark-submit --master spark://master:7077 /opt/bitnami/spark/bronze_data_to_s3.py
```

