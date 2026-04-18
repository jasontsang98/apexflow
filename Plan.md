
# Project Plan: Formula 1 Telemetry Lakehouse

## 1. Project Overview
This project aims to build a professional-grade end-to-end data pipeline using Formula 1 telemetry data from the OpenF1 API [cite: 148, 149]. The architecture follows a **Medallion Architecture** (Bronze, Silver, Gold) to demonstrate high-level Data Engineering (DE) and Data Analysis (DA) skills [cite: 151, 165].

## 2. Technical Stack
The project utilizes a modern, cloud-native stack optimized for high-frequency time-series data:
* **Ingestion:** Python (Requests + Pydantic for validation) [cite: 256, 264].
* **Landing Zone:** Google Cloud Storage (GCS) [cite: 246, 264].
* **Data Warehouse:** Google BigQuery [cite: 217, 264].
* **Transformation:** dbt (data build tool) [cite: 243, 264].
* **Orchestration:** Airflow (Running locally in Docker) [cite: 250, 264, 289].
* **Analysis & Visualization:** Streamlit (Hosted on Google Cloud Run) [cite: 260, 264].
* **Infrastructure:** Terraform (Infrastructure as Code) and GitHub Actions (CI/CD) [cite: 252, 262, 264].

## 3. Data Architecture (Medallion)

### Bronze Layer: Raw Ingestion [cite: 152]
* **Source:** OpenF1 API (Endpoints: `car_data`, `location`, `laps`, `stints`) [cite: 152].
* **Method:** A throttled Python ingestor fetches data in chunks to respect API rate limits (3 requests/sec) [cite: 168, 169].
* **Storage:** Data is saved as raw JSON or Parquet files in GCS [cite: 170, 221, 247, 282].
* **Objective:** Schema-on-read; ensuring pipeline resilience if API structures change [cite: 196, 197].

### Silver Layer: Cleansed & Standardized [cite: 155]
* **Process:** Flattening nested JSON into tabular formats using dbt and BigQuery SQL [cite: 157, 226].
* **Engineering Tasks:**
    * Standardize timestamps to UTC ISO 8601 [cite: 158, 174].
    * Enrich telemetry data by joining it with lap information [cite: 159, 174].
    * Implement data quality tests using `dbt-expectations` [cite: 257].
* **Optimization:** Tables are **Partitioned** by date and **Clustered** by driver/session to ensure cost efficiency within BigQuery's free tier [cite: 227, 279, 281].

### Gold Layer: Analytics & Business Intelligence [cite: 161]
* **Objective:** Create high-performance aggregate tables for "racing intelligence" [cite: 177, 205].
* **Key Metrics (KPIs):**
    * **Performance Deltas:** Track coordinates vs. time gap between drivers [cite: 178, 231].
    * **V-Min Analysis:** Minimum apex speeds per corner [cite: 179, 230].
    * **Tire Degradation:** Lap duration trends per tire compound [cite: 180].
    * **Driver Aggression:** Modeling throttle, brake, and gear usage patterns [cite: 181].

## 4. Operational Strategy & Constraints
* **BigQuery Free Tier:** Leveraging the 10 GB storage and 1 TB monthly query limits [cite: 268, 269].
* **Scalability:** Use of Parquet in GCS to reduce storage footprint and scan costs [cite: 282, 283].
* **Reliability:** Dockerized components ensure environment consistency across local and cloud deployments [cite: 249, 250].

## 5. Roadmap & Milestones [cite: 182]

### Phase 1: Foundation (Week 1)
* Set up Docker, Terraform, and GCS buckets.
* Develop the Bronze ingestor for historical race data (e.g., Abu Dhabi 2024) [cite: 183].

### Phase 2: Pipeline Development (Week 2)
* Implement Silver transformation logic in dbt [cite: 184].
* Establish Gold aggregate table schemas [cite: 185].

### Phase 3: Insights & Visualization (Week 3)
* Build interactive Streamlit dashboard [cite: 185].
* Deploy Streamlit to Google Cloud Run for public showcase [cite: 260].

## 6. Portfolio Impact
This project showcases mastery over:
* Handling **high-frequency time-series telemetry** [cite: 148, 188].
* Implementing **Cloud Data Warehouse best practices** (BigQuery) [cite: 218, 227].
* Building **automated, containerized CI/CD pipelines** [cite: 176, 262].
* Transforming raw data into **advanced sports analytics** [cite: 189].
