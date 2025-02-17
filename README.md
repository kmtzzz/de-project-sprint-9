# Sprint 9 project

### Description
This repository is intended for source code of Sprint 9 project.  

***Technologies used in implementation:***
1. Yandex Cloud manages services (each service deployed from scratch):
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

***Data model for DWH implementation***  
- Data vault

### Repository structure
Inside `solution` next folders exist:
- `/solution/service_dds` - implementation of microservice for reading data from STAGING Kafka topic and population of DDS layer in DWH.
- `/solution/service_cdm` - implementation of microservice for reading data from DDS Kafka topic and population of CDM layer in DWH.
- `/solution/ddl` - DDLs for database model definition.
- `/src/screenshots` -  screenshots with results of implementation.
