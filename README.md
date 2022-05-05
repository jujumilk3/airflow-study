# airflow-study

## Information
1. env
   1. python 3.9.12
2. conf location
   1. ~/airflow/...

## Commands
1. init  
   `airflow db init`
2. create user  
   ```shell
    airflow users create \
          --role Admin \
          --username admin \
          --email admin \
          --firstname admin \
          --lastname admin \
          --password admin
    ```
3. run  
   `airflow webserver --port 8080`  
   `airflow scheduler`
4. start webserver  
   `python webserver.py`
