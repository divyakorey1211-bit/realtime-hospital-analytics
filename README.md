Contents

🧠 Problem Context

🎯 Objectives

📁 Repository Structure

🛠 Tech Stack

🔄 Data Flow Architecture

⭐ Analytical Data Model

⚙️ Pipeline Logic

📊 Operational Dashboard

📈 Insights Delivered


🧠 Problem Context

Hospitals rarely struggle because of a single bottleneck. Operational pressure builds when patient arrivals, wait times, and length of stay compound across departments. Most reporting systems surface volume metrics but fail to explain why congestion persists or where efficiency breaks down.

This project focuses on operational patient flow, treating each patient interaction as an event and analyzing how flow dynamics evolve over time. The intent is to expose pressure points that are not obvious from static or batch-based reports.

🎯 Objectives

This system was designed to convert raw patient flow events into actionable operational insight.

Specifically, it aims to:

Capture patient activity as a continuous real-time stream

Enforce data quality before analytics consumption

Model data for operational analysis rather than raw reporting

Enable flexible analytics without duplicating data

Present insights that support capacity planning and process optimization

The focus is on understanding system behavior, not just measuring outcomes.

📁 Repository Structure

The repository is organized to reflect the logical stages of a real-time analytics system, from event generation to insight delivery:

real-time-patient-flow-analytics/
│
├── simulator/
│   └── patient_event_producer.py
│   # Generates real-time patient flow events
│
├── databricks-notebooks/
│   ├── 01_eventhub_stream_ingestion.py
│   ├── 02_data_validation_and_cleansing.py
│   └── 03_analytics_data_model.py
│   # Streaming ingestion, data quality enforcement, analytics modeling
│
├── sqlpool-queries/
│   └── SQL_pool_queries.sql
│   # Serverless SQL views for analytics access
│
├── power-bi/
│   └── healthcare.pbix
│   # Operational dashboard built on analytical views
│
├── .gitignore
└── README.md


Each layer is isolated by responsibility, making the system easier to reason about, modify, and version using Git.

🛠 Tech Stack

Each component exists for a clear reason:

* Azure Event Hub → scalable real-time ingestion

* Azure Databricks (PySpark) → streaming transformation & validation

* Azure Data Lake Storage Gen2 → durable lakehouse storage

* Azure Synapse Serverless SQL → analytics access without infrastructure overhead

* Power BI → controlled delivery of insights

* Git & GitHub → version control, traceability, and collaboration

The stack supports real-time analytics with engineering discipline, not overengineering.

🔄 Data Flow Architecture

The system follows an event-driven, layered design:

🥉 Raw Event Capture (Bronze)

Stores raw JSON events

Preserves source payloads

Enables replay and traceability

🥈 Validated Events (Silver)

Schema enforcement

Invalid age correction

Future timestamp handling

Timestamp consistency guarantees

🥇 Analytical Tables (Gold)

Fact and dimension tables

Business-ready structure

Optimized for aggregation and slicing

⭐ Analytical Data Model

The Gold layer implements a simple star schema:

📌 Fact

Patient events
(admission time, discharge time, wait time, length of stay)

📌 Dimensions

Patient

Department

The model is intentionally minimal. Advanced historical modeling was avoided to keep analytics clear, stable, and defensible.

⚙️ Pipeline Logic

Instead of step-by-step setup instructions, the pipeline is organized by capability:

Streaming Ingestion
Continuous event consumption via a Kafka-compatible interface

Data Validation & Enrichment
Business-rule enforcement and anomaly handling

Analytics Modeling
Fact and dimension construction with surrogate keys

Analytics Access
Serverless SQL views over Delta tables (schema-on-read)

All pipeline changes are tracked through Git, enabling safe iteration and rollback.

📊 Operational Dashboard

The Power BI dashboard is intentionally one page.

Each visual answers a specific operational question:

How does patient demand evolve over time?

Is throughput efficiency aligned with demand?

Which departments experience the highest pressure?

When does demand peak during the day?

Where is patient experience most at risk?

Design Principles

Time-based metrics normalized to hours

Percentile-based indicators for tightly clustered values

Minimal slicers to avoid over-filtering

No decorative or redundant visuals

📈 Insights Delivered

The system enables several actionable insights:

Operational pressure is not explained by admissions volume alone

Average wait time is relatively uniform across departments

Length of stay is the primary driver of experience risk

Some departments absorb higher demand without proportional inefficiency

Demand peaks at specific hours, revealing staffing misalignment

These insights shift focus from “we’re busy” to “where are we inefficient?”.

