# NYC_taxi_data_pipeline
This project is an implementation of ETL pipeline to extract NYC taxi data available publicly at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page, apply all the necessary transformations and transform data into fact and dimesion tables. Then load these tables into PostgreSQL. Airflow was used to orchestrate this pipeline.

## Technologies Used
- Python
- Apache Spark
- Airflow
- Docker
- PostgreSQL

## Data extraction
Downloaded 2023 taxi data in parquet format from the url using python requests 

## Transformation
- Inspected the data, applied transformations like
    - casting
    - removing or replacing missing values
    - replacing mapped values
    - splitting data into dimension tables
    - creating the fact table
  
## Loading data
- Saved data into PostgreSQL running in a docker container that comes with airflow.

## Steps to run the pipeline
- Using the following link, download the worker_with_spark.tar file and load it into docker to use as image for airflow worker.
```bash
https://drive.google.com/file/d/1pLiUm-BmwZOC278W9QlZyOVUWfhIPGGh/view?usp=drive_link
```
- Start the docker containers by running the following command in the folder airflow
    ```bash
    docker-compose up -d
    ```
- Login to the shell of airflow scheduler and install pyspark using pip.
- using the following command get the IP address of the postgreSQL container
  ```bash
  docker inspect <PostgreSQL container_id>
  ```
- create a config.yaml file in the config folder in airflow and save the connection paramas in the below format
  ```bash
  connection_params:
    database: "nyc_taxi_data"
    host: <PostgreSQL IP address>
    user: <Username of PostgreSQL>
    password: <Password of PostgreSQL>
    port: 5432
  ```
- If you have access to more memory, go ahead and remove the line in the source code **src.py** that says yellow_df.limit() to run the pipeline on whole dataset. I had to limit this since I'm running on my local machine and have only limited resources.
- open your browser and go to localhost:8080 to access the airflow web UI.
- Login and run the DAG **nyc_taxi_data** to execute the pipeline.
- The graph of the pipeline should look like this.
  ![Alt airflow pipeline](https://github.com/DSK2012/NYC_taxi_data_pipeline/blob/main/images/airflow_pipeline.jpeg)
- Once the pipeline is done executing, go back to terminal and check if the data is populated in the PostgreSQL docker container using the shell.
- These images below is the data populated in PostgreSQL databse **nyc_taxi_data**
  ![Alt tables](https://github.com/DSK2012/NYC_taxi_data_pipeline/blob/main/images/tables.jpeg)
  ![Alt table_data](https://github.com/DSK2012/NYC_taxi_data_pipeline/blob/main/images/table_data.jpeg)
  ![Alt fact_table](https://github.com/DSK2012/NYC_taxi_data_pipeline/blob/main/images/fact_table.jpeg)
# Next Steps
- Implement this on cloud or use a computer resource from cloud.
- Build a dashboard to display analytics.
