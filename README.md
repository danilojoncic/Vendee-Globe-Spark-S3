# Vendee-Globe-Spark-S3
Dockerized "Data-Lake" with MinIO serving as S3 object store, nginX as load balancer, using python scripts to load and convert files within S3 and Spark workers to transform files 
Implementation of a previous project [Vendee-Globe-Data-Analysis](https://github.com/danilojoncic/Vendee-Globe-Data-Analysis) with Apache Spark, Scala and MinIO (S3).

## Setup Visualization
<img width="1068" height="553" alt="Screenshot 2025-11-27 at 14 47 25" src="https://github.com/user-attachments/assets/e367502a-dfb5-4251-8c9d-1db8b488489e" />

Project files 
```
config/
    └── nginx.conf <------------------------------ load balancer config
docker/
    ├── docker-compose-minio-test.yaml <---------- docker compose setup for local running and testing with Spark in local mode with an IDE
    ├── docker-compose.yaml <--------------------- full docker compose with all services
    ├── Dockerfile-convert <---------------------- python image with script
    └── Dockerfile-fetch       <-------------------python image with scirpt
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

