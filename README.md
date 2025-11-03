### Setup instructions

First, these all prerrequisites to run the code:

- Python 3.8+ needed
- PostgreSQL 13+ needed with a database named vip_medical_group
- pgAdmin to run the DDL

### Configure environment:

- Create and activate a virtual env:

py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

- Create .env from the example file and adjust values (Keep paths in double quotes because they contain spaces and if your password has special characters, URL-encode them.)

- Load the database by running sql/ddl.sql in pgAdmin

- Copy the two .xlsx files into data/raw/ (matching the .env paths)

- Run the pipeline with python -m src.etl

- (In pgAdmin) validate the load with SELECT COUNT(*) FROM healthtech.doctors;
SELECT COUNT(*) FROM healthtech.appointments;

- Run the assesment queries by executing sql/queries.sql in pgAdmin

### Pipeline explanation:

- Local Python only ETL.

- Cleans typos and different names for a single thing by normalizing in config.py

- Idempotent full refresh (using TRUNCATE/LOAD).

- Clear logging to console and file by isolating bad rows from the final result

- etl.py orchestrates the whole process (extract, transform, load)


**Extract**

- Files: two Excel workbooks (Doctors, Appointments).

- Reader: pandas.read_excel(engine="openpyxl").

- Guards: path existence check and logs rows/columns count.

**Transform**

- Performed in src/transform.py 

- booking_id: strip all non digits, cast to Int64. Rows still null get dropped (logged).

- patient_id: cast to Int64. Nulls are dropped (logged).

- doctor_id: cast to numeric.

Dates (booking_date)

- Robust parsing into Python date objects instead of Timestamp to avoid the 2262-04-11 ceiling in datetime64.

- Supported formats: YYYY-MM-DD, YYYY/MM/DD, MM/DD/YYYY and Excel serial numbers (origin 1899-12-30).

- Unrealistic futures are dropped by rule: any date further than 2070-01-01.

- Any rows still invalid get previewed in logs and fully dumped to logs/invalid_booking_date_rows.csv.

Status normalization

- Map to the closed set {confirmed, cancelled}:

- canceled -> cancelled
 
- confirmed. -> confirmed

- Distribution before/after is logged for transparency.

Quality

- PK uniqueness: doctors.doctor_id, appointments.booking_id.

- NOT NULL: appointments.doctor_id.

- Domain: status is {confirmed, cancelled}.

- Valid date: booking_date not null (after parsing and filters).

FK enforcement

- Check explanation for doctors FK under business rules

Outputs

- Persist exact final inputs to load as CSV:

data/processed/final_doctors.csv

data/processed/final_appointments.csv

**Load**

- Connection: psycopg2, masked DSN in logs, connect_timeout.

- Idempotent full refresh: one transaction:

- TRUNCATE healthtech.appointments, healthtech.doctors;

- Bulk insert doctors, then appointments (execute_values, batched).

- Commit or rollback on error.

**Observability & Audit**

Console file logger: logs/pipeline.log

Data rejection logs:

FK quarantine: rejected_appointments_fk_doctor_missing.csv

Date parsing failures: invalid_booking_date_rows.csv

Counts in logs after each stage

### AWS Architecture Proposal (Production):

**Storage & Catalog**

Amazon S3 as the lake:

- s3://vip-medical-group/bronze/appointments/… (raw Excel)

- s3://vip-medical-group/silver/… (cleaned parquet or csv)

- s3://vip-medical-group/gold/… (model ready)

AWS Glue Data Catalog for schemas and Glue Crawlers on Bronze/Silver folders.

Justification: durable, cheap, standard for staged data. Glue data catalog makes schema evolution posible

**Extract**

* Files via SFTP: AWS Transfer Family to S3 (Bronze).

* Files via HTTP or API: Amazon API Gateway and AWS Lambda to write to S3.

* Batch uploads from partners: S3 URLs or direct S3 access with IAM.

Justification:endpoints that drop into S3 with low effort

**Transform & Data Quality**

AWS Glue Jobs:

- Python Shell job for small or medium datasets (similar to our local pandas/Python).

- Spark ETL job if volume grows

Data quality:

AWS Glue Data Quality in Glue jobs to enforce:

- PK uniqueness, domain constraints, date validity

- FK checks against a reference dataset (i.e. doctors master in Silver/Gold)

Quarantine:

- Violations land in s3://…/quarantine/ with reason codes (FK_DOCTOR_MISSING, INVALID_DATE, etc...)

Justification: Glue is serverless, integrates with Catalog, scales when needed, and supports Python-only or Spark pipelines.

**Orchestration & Scheduling** 

Amazon EventBridge to trigger on ObjectCreated.

AWS Step Functions to orchestrate the stages:

- Validate file (Lambda)

- Run Glue Transform

- Run Data Quality

- Load to DB

- Notify with metrics and links to quarantine objects

justification: Simple, allows retries/idempotency and native service integrations, EventBridge allows triggers based on events

**Load**

Amazon RDS PostgreSQL for the serving database (similar to local Postgres).

AWS Secrets Manager for DB credentials used in Glue or Lambda when running

Why: Postgres is simple, Secrets Manager allows rotation and patterns map almost exactly to local behavior.

**Monitoring, Alerting, Security**

CloudWatch for Glue and Lambda; alarms to SNS (email)

S3 access logs for audit

IAM least privilege for private S3 access.

Idempotency: S3 object keys include run IDs; Step Functions has native idempotency; optionally use DynamoDB for run locks/checkpoints.

### Business rules and data quality
- **Dates >= 2070-01-01** while we didn't have specific business rules for these, it is unrealistic to assume that doctors would live further than 2070, for instance. In a real-life scenario I would evaluate with superiors before taking this decision.
- **Unexistent doctors FKs**: appointments got **persisted** in `logs/rejected_appointments_fk_doctor_missing.csv` and will **not** be loaded until correcting master data: there were more than a hundred records of a doctor identified as 105 which makes this unlikely to be a typo. Taking into account that the appointments list could be ahead of the doctors list, a whole .csv was created for tracking this data instead of just deleting it.
- **booking_id**: non-numerical data was cleaned.
- **null patient_id**: is discarded with a warning, we can't assume unknown IDs in such a delicate area.