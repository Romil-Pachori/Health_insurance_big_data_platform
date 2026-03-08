# Health Insurance Big Data Platform for Analytics and Risk Monitoring

## Project Context

This project is being built as a **portfolio-grade Data Engineering project** tailored for a **Data Engineer Apprenticeship in Public Services**. The target role involves contributing to a **Big Data platform** used by **Data Scientists and Data Analysts** working with **health insurance and public-sector data**.

The project should simulate a real-world environment where analysts and data scientists need access to clean, reliable, analytics-ready datasets built from raw operational data.

The goal is to build a **mini CNAM-style Big Data platform** using modern data engineering tools and practices.

---

## Core Goal

Build a **containerized, modular, analytics-ready health insurance data platform** that:

- ingests raw healthcare and insurance-related datasets
- cleans and standardizes them
- transforms them into curated analytical models
- applies automated data quality checks
- orchestrates the workflow end-to-end
- enables downstream usage through Jupyter notebooks and SQL analytics

This is not just a script collection. It must look like a **real data platform project**.

---

## Main Business Use Cases

The platform should support the following use cases:

### 1. Claims Analytics
Prepare high-quality claims datasets for:
- reimbursement trend analysis
- cost monitoring
- claims volume tracking
- category-level claim summaries

### 2. Patient Journey Analytics
Prepare datasets that allow users to analyze:
- consultations
- prescriptions
- hospital visits
- reimbursements over time

### 3. Anomaly Detection Preparation
Create curated datasets that can later be used for:
- duplicate claim detection
- suspicious provider behavior analysis
- unusually high reimbursement analysis

### 4. Regional Public Health Reporting
Enable aggregate reporting by:
- region
- provider type
- treatment category
- time period

---

## End Users

The platform is being designed for the following simulated users:

- **Data Analysts** who need clean SQL-ready tables
- **Data Scientists** who need curated datasets for experiments and notebooks
- **Technical Leads / Architects** who care about modularity, testing, and maintainability

---

## Project Principles

The project must follow these principles:

- modular and maintainable
- production-style folder structure
- containerized local environment
- reproducible workflows
- clear separation of raw, staging, and curated layers
- data quality built into the pipeline
- documentation-first mindset
- simple enough to run locally, but designed as if it could scale

---

## Scope

### In Scope
- local containerized platform using Docker Compose
- Python-based ingestion and transformation logic
- Spark-based transformation jobs
- Airflow orchestration
- PostgreSQL serving/analytics layer
- Jupyter notebooks for downstream analysis
- synthetic or open public health / insurance style datasets
- data quality checks
- star-schema or analytics-ready curated models
- tests for core transformations and validations

### Out of Scope
- real patient data or protected health information
- enterprise deployment to real OpenShift clusters
- production-grade security/authentication
- frontend web app
- overly complex distributed infrastructure

---

## Recommended Tech Stack

### Core
- **Python**
- **Apache Spark**
- **Apache Airflow**
- **PostgreSQL**
- **Jupyter Notebook**
- **Docker / Docker Compose**
- **Git**

### Optional / Stretch
- Great Expectations
- dbt-style transformation layer
- parquet storage
- MinIO for object storage simulation
- CI checks
- simple monitoring/logging enhancement

---

## Functional Requirements

The platform must implement the following functional flow:

1. Load raw input datasets into the raw layer
2. Validate schema presence and basic file integrity
3. Clean and standardize records
4. Build analytics-ready fact and dimension tables
5. Run data quality checks on curated outputs
6. Load curated tables into PostgreSQL
7. Make curated data available for notebooks / analysis
8. Orchestrate all tasks using Airflow

---

## Proposed Data Domains

The project should simulate these healthcare/public-service datasets:

- `patients`
- `providers`
- `claims`
- `prescriptions`
- `hospital_visits`
- `regions`
- `treatments`

### Example Columns

#### patients
- patient_id
- birth_date
- gender
- region_id
- registration_date

#### providers
- provider_id
- provider_name
- provider_type
- region_id

#### claims
- claim_id
- patient_id
- provider_id
- treatment_id
- claim_date
- billed_amount
- reimbursed_amount
- claim_status

#### prescriptions
- prescription_id
- patient_id
- provider_id
- drug_name
- prescription_date
- quantity

#### hospital_visits
- visit_id
- patient_id
- provider_id
- admission_date
- discharge_date
- diagnosis_code

#### regions
- region_id
- region_name
- population_group

#### treatments
- treatment_id
- treatment_category
- treatment_name
- standard_cost

---

## Data Architecture

The project should follow a layered architecture.

### 1. Raw Layer
Purpose:
- store raw source data as-is
- preserve original structure for traceability

Examples:
- CSV files
- JSON files
- parquet files

### 2. Staging Layer
Purpose:
- clean and standardize the raw data
- enforce schema consistency
- normalize dates, IDs, text fields, and missing values

### 3. Curated Layer
Purpose:
- generate analytics-ready models
- build fact and dimension tables
- optimize for reporting and notebook usage

### 4. Consumption Layer
Purpose:
- expose datasets to analysts/data scientists
- allow SQL access and notebook analysis

---

## Target Data Model

Use a simple analytics-oriented model.

### Fact Tables
- `fact_claims`
- `fact_prescriptions`
- `fact_visits`
- `fact_reimbursements`

### Dimension Tables
- `dim_patient`
- `dim_provider`
- `dim_region`
- `dim_treatment`
- `dim_date`

### Modeling Expectations
- stable primary/business keys
- standardized foreign keys
- clean datatypes
- analytics-friendly naming
- ready for filtering, grouping, and aggregation

---

## Pipeline Modules

The codebase should be organized around clear modules.

### Module 1: Ingestion
Responsibilities:
- load raw files into the raw zone
- validate file presence
- perform initial schema checks
- log ingestion events

### Module 2: Cleaning and Standardization
Responsibilities:
- handle nulls
- standardize timestamps
- deduplicate records
- normalize categorical values
- validate IDs and dates

### Module 3: Spark Transformations
Responsibilities:
- transform staged datasets into curated models
- build fact tables
- build dimension tables
- apply business rules

### Module 4: Data Quality
Responsibilities:
- uniqueness checks
- null checks on mandatory fields
- referential integrity checks
- numeric range checks
- row count reconciliation

### Module 5: Orchestration
Responsibilities:
- define Airflow DAG
- manage task order and dependencies
- run pipeline end-to-end

### Module 6: Analytics / Notebook Layer
Responsibilities:
- provide sample notebooks
- show query examples
- demonstrate business insights from curated data

---

## Data Quality Expectations

Data quality is a core feature, not an afterthought.

At minimum, implement checks for:

- duplicate IDs
- nulls in mandatory columns
- valid timestamp parsing
- non-negative billed/reimbursed amounts
- reimbursed amount not exceeding billed amount unless explicitly allowed
- referential integrity across fact/dimension joins
- valid enumerated statuses where applicable
- row count comparisons between stages

The project should fail gracefully or log meaningful errors when validations fail.

---

## Orchestration Expectations

Use **Apache Airflow** to orchestrate the pipeline.

The DAG should include tasks like:

1. ingest raw datasets
2. clean patients
3. clean providers
4. clean claims
5. clean prescriptions
6. clean visits
7. build dimensions
8. build facts
9. run quality checks
10. load to PostgreSQL
11. notify success/failure in logs

The DAG should be readable and modular.

---

## Containerization Expectations

Use **Docker Compose** to define the local environment.

Expected services:
- PostgreSQL
- Airflow webserver
- Airflow scheduler
- Spark
- Jupyter
- optional MinIO or supporting service if needed

The environment should be easy to run locally.

Commands should be simple and documented.

---

## Suggested Repository Structure

```text
health-insurance-data-platform/
│
├── data/
│   ├── raw/
│   ├── staging/
│   └── curated/
│
├── dags/
│   └── health_pipeline_dag.py
│
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── quality/
│   ├── utils/
│   └── config/
│
├── spark_jobs/
│   ├── clean_claims.py
│   ├── clean_patients.py
│   ├── clean_providers.py
│   ├── build_dimensions.py
│   └── build_facts.py
│
├── sql/
│   ├── ddl/
│   ├── analytics/
│   └── quality_checks/
│
├── notebooks/
│   ├── analyst_exploration.ipynb
│   └── claims_reporting.ipynb
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformations.py
│   ├── test_quality.py
│   └── test_schema.py
│
├── docker/
│   └── Dockerfile
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .env.example