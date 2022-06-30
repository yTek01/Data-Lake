# Data-lake-ETL

Here is the thing, I am going to build the all the Docker images, and then rum and provision the Docker containers needed for our deployments. Individual instructions are in each services (folder). 


* Build the Spark image; the master and the worker node. 
```BASH
docker build -f data_lakehouse/Dockerfile.Spark . -t spark-air
```

* Build Apache Airflow Docker image.
```BASH
docker build -f batch_processing_apache-airflow/Dockerfile.Airflow . -t airflow-spark
```

* Build the RPA Docker image.
```BASH
docker build -f RPA/Dockerfile.RPA . -t rpa_selenium_image
```

* Start every of the data engines.
```BASH
docker-compose -f data_lakehouse/docker-compose.Lakehouse.yaml -f yugabytesDB/docker-compose.Yugabyte.yaml -f lakefs/docker-compose.LakeFS.yaml -f batch_processing_apache-airflow/docker-compose.Airflow.yaml -f RPA/docker-compose.RPA.yaml up -d
```







Apache Spark and YugabytesDB

```

```

```BASH
docker-compose -f data_lakehouse/docker-compose.Lakehouse.yaml -f yugabytesDB/docker-compose.Yugabyte.yaml up -d
```