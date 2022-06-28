# Yugabyte Cluster Database Deployment


1. Start the Cluster

```bash
docker-compose -f ./docker-compose.Yugabyte.yaml up -d
```


2. Initialize the APIs
YCQL and YSQL APIs are enabled by default on the cluster.


3. Test the APIs
Clients can now connect to the YSQL API at localhost:5433 and the YCQL API at localhost:9042. The yb-master admin service is available at http://localhost:7000.

4. Connect to YSQL
```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1
```



```bash
CREATE TABLE foo(bar INT PRIMARY KEY);
```



5. Connect to YCQL
```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ycqlsh yb-tserver-n1
```

```bash
CREATE KEYSPACE mykeyspace;
```

```bash
CREATE TABLE mykeyspace.foo(bar INT PRIMARY KEY);
```

```bash
DESCRIBE mykeyspace.foo;
```

4. Stop the cluster
```bash
docker-compose -f ./docker-compose.yaml down
```

To open the YSQL shell, run ysqlsh.
```bash
docker exec -it yugabyte /home/yugabyte/bin/ysqlsh --echo-queries
```

```bash
CREATE DATABASE yb_demo;
```

```bash
\c yb_demo;
```

```bash
CREATE TABLE IF NOT EXISTS public.dept (
    deptno integer NOT NULL,
    dname text,
    loc text,
    description text,
    CONSTRAINT pk_dept PRIMARY KEY (deptno asc)
);
```



```bash
curl -O https://www.postgresqltutorial.com/wp-content/uploads/2019/05/dvdrental.zip
```

```bash
sudo apt install unzip
```

```bash
unzip dvdrental.zip
```

```bash
tar -xf dvdrental.tar
```

```bash
\! pwd
```

```bash
docker cp dvdrental.tar yb-tserver-n1:/home/yugabyte/
```

```bash
docker cp 3055.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3057.dat yb-tserver-n1:/home/yugabyte/ \ 
docker cp 3059.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3061.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3062.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3063.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3065.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3067.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3069.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3071.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3073.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3075.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3077.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3079.dat yb-tserver-n1:/home/yugabyte/ \
docker cp 3081.dat yb-tserver-n1:/home/yugabyte/ \
docker cp toc.dat yb-tserver-n1:/home/yugabyte/ \
docker cp restore.sql yb-tserver-n1:/home/yugabyte/ \
```

```bash
docker cp restore.sql yb-tserver-n1:/home/yugabyte/ 
```

```
docker cp restore.sql postgres:/
```


```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1
```

* Load the data into the database.
```bash
\i restore.sql
```

```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1
```

```bash
docker exec -it yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1 -U postgres -d dvdrental
```

* List all the tables in the dvdrental database. 
```bash
\dt
```


* Check the actors table.
```bash
SELECT * FROM public.actor;
```

* Check all the Customers table.
```bash
SELECT * FROM public.customer;
```