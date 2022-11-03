
### Data Lake Setup

* Navigate to the ./data_lake directory and execute the command below.

```BASH
docker build -f Dockerfile.Spark . -t spark-air
```

* Now start the Data Lake containers
```BASH
docker-compose -f docker-compose.Lakehouse.yaml up -d
```

* You can update and install the Spark Packages Seperately

```BASH
docker exec -it master bash /opt/workspace/dependencies/packages_installer.sh 
```

* Run Spark Jobs.
```BASH
docker exec -it master spark-submit --master spark://master:7077 /opt/bitnami/spark/data_warehousing_script.py
```

