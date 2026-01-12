
CREATE DATABASE hospital_analytics;
USE hospital_analytics;
FACT VIEW
--CREATE OR ALTER VIEW vw_fact_patient_event AS
SELECT
    fact_id,
    patient_sk    AS patient_key,
    department_sk AS department_key,
    admission_time,
    CAST(admission_time AS DATE) AS admission_date,
    discharge_time,
    wait_time_minutes,
    length_of_stay_hours,
    is_currently_admitted,
    bed_id
FROM OPENROWSET(
    BULK 'https://<storage_account>.dfs.core.windows.net/gold/fact_patient_event/',
    FORMAT = 'DELTA'
) AS f;

--PATIENT DIMENSION
CREATE OR ALTER VIEW vw_dim_patient AS
SELECT
    patient_sk AS patient_key,
    patient_id,
    gender,
    age
FROM OPENROWSET(
    BULK 'https://<storage_account>.dfs.core.windows.net/gold/dim_patient/',
    FORMAT = 'DELTA'
) AS p;

--DEPARTMENT DIMENSION
CREATE OR ALTER VIEW vw_dim_department AS
SELECT
    department_sk AS department_key,
    department,
    hospital_id
FROM OPENROWSET(
    BULK 'https://<storage_account>.dfs.core.windows.net/gold/dim_department/',
    FORMAT = 'DELTA'
) AS d;

