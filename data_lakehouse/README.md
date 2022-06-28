


* Navigate to the ./data_lakehouse directory and execute the command below.

```
docker build -f Dockerfile.Spark . -t spark-air
```

* Now start the Lakehouse containers
```
docker-compose -f docker-compose.Lakehouse.yaml up -d
```

* Go to https://9001-devilisdefe-deltalakeet-1na534igs3o.ws-eu47.gitpod.io/login to check that MinIO is running. Login is provided below.

```
USER=admin
PASSWORD=123456789
```

* Create a Postgres database, we are going to name it CarParts and use CarParts.sql file to create tables).
```
docker exec -it postgres /bin/sh
psql -U root -d CarParts -W
password: root
```


Use the SQL commands in CarParts.sql file to create the tables and `\dt` to confirm the list. 

docker exec -it master bash /opt/workspace/dependencies/packages_installer.sh 


* Create an S3 Bucket
```

```


docker exec -it master spark-submit --master spark://master:7077 /opt/workspace/postgres_to_s3.py


