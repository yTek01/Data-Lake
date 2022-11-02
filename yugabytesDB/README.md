# Yugabyte (Postgres) Cluster Database Deployment (DEV)


### Start the Cluster
This is for individual setup. 
```bash
docker-compose -f ./docker-compose.Yugabyte.yaml up -d
```

### Initialize the APIs
YCQL and YSQL APIs are enabled by default on the cluster.

### Test the APIs
Clients can now connect to the YSQL API at localhost:5433 and the YCQL API at localhost:9042. The yb-master admin service is available at http://localhost:7000.

### Copy the database setup resources to the Dev machine 
```bash
docker cp postgres_resources.sql yb-tserver-n1:/home/yugabyte/ 
```

### Connect to YSQL
```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1
```

### Create the schema and the tables using the command below.
```bash
\i postgres_resources.sql
```
### Log out of the server. 
```BASH
exit
```

### Go into the container and confirm everything is fine
```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1 -U postgres -d Postgres
```

### List all the tables in the Postgres database. 
```bash
\dt *.*
```

### Log out of the server. 
```BASH
exit
```
