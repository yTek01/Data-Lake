# Data-lake-ETL

Here is the thing, I am going to build the all the Docker images, and then ruN and provision the Docker containers needed for our deployments. Individual instructions are in each services (folder). 

### Build the Spark image; the master and the worker node. 
```BASH
docker build -f data_lakehouse/Dockerfile.Spark . -t spark-air
```

### Build Apache Airflow Docker image.
```BASH
docker build -f batch_processing_apache-airflow/Dockerfile.Airflow . -t airflow-spark
```

### Start every of the data engines: Airflow, Spark, YugabyteDB (Postgres).
```BASH
docker-compose -f data_lakehouse/docker-compose.Lakehouse.yaml -f yugabytesDB/docker-compose.Yugabyte.yaml -f batch_processing_apache-airflow/docker-compose.Airflow.yaml up -d
```

### Access Spark
```BASH
http://localhost:8090/
```

### Access YugabyteDB (Postgres)
```BASH
http://localhost:7000/
```

### Access Airflow
Wait for Airflow to start fully.
```BASH
http://localhost:8080/
```

### Go into the YugabyteDB folder and set up the Database resources. 