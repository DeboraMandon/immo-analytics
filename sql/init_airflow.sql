-- Création utilisateur et bases pour Airflow et Metabase

CREATE USER airflow WITH PASSWORD 'airflow_secret';

CREATE DATABASE airflow
    WITH OWNER airflow
    ENCODING 'UTF8';

CREATE DATABASE metabase
    WITH OWNER immo_user
    ENCODING 'UTF8';

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;