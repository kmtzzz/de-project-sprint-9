# Sprint 9 project

### Description
This repository is intended for source code of Sprint 9 project.  

***Technologies used in implementation:***
1. Yandex Cloud manages services:
   1. Kafka
   2. Redis
   3. Postgres
   4. Kubernetes
   5. Container registry
   6. DataLens (BI)
3. Python
4. Docker
5. Docker-compose
6. Helm

### Repository structure
Inside `src` next folders exist:
- `/src/dags` - DAG, which extracts data from S3 and loads to Vertica STAGING data layer, DAG name is `1_data_import.py`. DAG updating data mart, DAG name is `2_datamart_update.py`.
- `/src/sql` - DDLs for database model definition in `STAGING`- and `DWH`- layers, as well as script for datamart upload.
- `/src/py` - in case of Kafka is source, place code for producing and consuming messages from it.
- `/src/img` -  screenshot of Metabase dashboard built on top of datamart.
