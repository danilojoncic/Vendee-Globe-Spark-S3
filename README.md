# Vendee-Globe-Spark-S3
Dockerized "Data-Lake" with MinIO serving as S3 object store, nginX as load balancer, using python scripts to load and convert files within S3 and Spark workers to transform files.
Implementation of a previous project [Vendee-Globe-Data-Analysis](https://github.com/danilojoncic/Vendee-Globe-Data-Analysis).

The services in this Dockerized data-lake start in the following order, respecting dependencies and health checks:

```
MinIO Cluster (all start in parallel):
├── minio1
├── minio2
├── minio3
└── minio4
|
v
Load Balancer:
└── nginx
├── data_fetch
└── spark-master
|
v
Spark Workers:
├── spark-worker-1
└── spark-worker-2
|
v
CSV Converter:
└── csv_convertor
```
**Notes:**
- All MinIO nodes must be healthy before `nginx` starts.  
- `spark-master` starts after `nginx` is ready.  
- `spark-workers` wait for `spark-master` to be healthy.  
- `csv_converter` waits for both `spark-master` and `nginx`.  
- `data_fetch` depends on `nginx` only. 




## Setup Visualization
<img width="1068" height="553" alt="Screenshot 2025-11-27 at 14 47 25" src="https://github.com/user-attachments/assets/e367502a-dfb5-4251-8c9d-1db8b488489e" />

## Project files 
```
config/
    └── nginx.conf <------------------------------ load balancer config
docker/
    ├── docker-compose-minio-test.yaml <---------- docker compose setup for local running and testing with Spark in local mode with an IDE
    ├── docker-compose.yaml <--------------------- full docker compose with all services
    ├── Dockerfile-convert <---------------------- python image with script
    └── Dockerfile-fetch <------------------------ python image with scirpt
sample_data/
    ├── leaderboard_20241110_220000-4.csv <------- output of csv_converter.py
    ├── leaderboard_20241110_220000.xlsx <-------- downloaded file using data_fetch.py
    └── part-00000-....snappy.parquet <----------- .parquet file saved using Spark with a schema
scala/
    ├── untitled3/
        ├── .bsp/
            └── sbt.json
        ├── project/
            ├── build.properties
            └── plugins.sbt <--------------------- necessary plugin to enable sbt assembly (for building the fat jar)
        ├── src/
            └── main/
                └── scala/
                    ├── Configurer.scala
                    ├── FullBucket.scala
                    ├── Join.scala
                    ├── ListingAllFiles.scala
                    ├── OneFile.scala
                    ├── Schemas.scala
                    └── ToPostgresWithScheme.scala
        └── build.sbt <-------------------------- build config file with all dependencies named
    ├── required_vm_options.txt <---------------- necessary vm options if runnning from an IDE like IntelliJ
    └── running.txt <---------------------------- simple instructions on how to run
scripts/
    ├── convert_to_csv.py
    ├── fetch_data.py
    └── requirements.txt <----------------------- python scripts dependencies 
README.md <-------------------------------------- you are here!
```

## Available ports
Following are the ports that are available outside the docker container
- localhost:8080 Spark Master UI
- localhost:4040 Spark UI
- localhost:9001 MinIO console

## Requirements

- Docker & Docker Compose  
- Java JDK 8 (for Scala 2.12.18 / Spark 3.5.7)  
- sbt (Scala Build Tool 1.17.x.x)  
- Python 3.x (for data fetch/conversion scripts)  

---

## How to Run

**Recommended:** Use separate terminal tabs for resource usage (`docker stats`), Docker Compose logs, and Spark master commands.

### Local IDE
1. Clone the entire repository.  
1. Navigate to `/docker` and start the cluster with the following command
    ```bash
   docker compose -f docker-compose-minio-test.yaml up`
    ```
3. Wait for the MinIO cluster and load balancer to start and become healthy.  
4. Paste the `scala` directory in your IDE with a Scala project.  
5. Wait for sbt to load and build.  
6. Run one of the Scala scripts (three contain a `main` method).  
7. Observe output in the console or check the MinIO console for bucket/file creation.

### Full Docker Environment

1. Clone the entire repository.
2. Navigate to `/docker` and start the cluster.  
3. Run `docker-compose up` with the full YAML.  
4. Wait for all services to start and become healthy.  
5. Navigate to the `scala` directory and run `sbt assembly` to package the code into a fat jar.  
6. Copy the fat jar into the Spark master container:
   ```bash
   docker cp path/to/fatjar.jar spark-master:/opt/jars/
7. Access the Spark master container:
   ```bash
    docker exec -it spark-master bash
    cd /opt/spark/bin
   ```
8. Submit the jar to be run in the cluster:
```bash
    ./spark-submit --class <MainClass> ../jars/fatjar.jar
```
8. Monitor the console for output or check the Spark UI for jobs, stages, and tasks.
