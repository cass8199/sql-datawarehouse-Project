**Data Warehouse and Analytics Project**

G'Day to every one who is on my Data Warehouse and Analytics Project Repository! =) 
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

**📘 Project Overview**

This project involves:

Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.

ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.

Data Modeling: Developing fact and dimension tables optimized for analytical queries.

Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

* SQL Development
* Data Architect
* Data Engineering
* ETL Pipeline Developer
* Data Modeling
* Data Analytics

🚀 **Project Requirements**

Building the Data Warehouse (Data Engineering)

Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
* Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
* Data Quality: Cleanse and resolve data quality issues prior to analysis.
* Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
* Scope: Focus on the latest dataset only; historization of data is not required.
* Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

**Data Architecture**
![Image](https://github.com/user-attachments/assets/9a5fa87e-c17b-4414-8f9b-096a72da8bd6)

1. **Bronze Layer**: Stores raw data as is from the source systems. Data is ingested from csv files into SQL server Database.
2. **Silver Layer**: This Layer includes data cleansing, Standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics. 

📁 **Repository Structure**

📁 Repository Structure

data-warehouse-project/
├── datasets/                         # Raw datasets used for the project (ERP and CRM data)
├── docs/                             # Project documentation and architecture details
│   ├── etl.drawio                    # Draw.io file shows all different techniques and methods of E
│   ├── data_architecture.drawio     # Draw.io file shows the project’s architecture
│   ├── data_catalog.md              # Catalog of datasets, including field descriptions and metadat
│   ├── data_flow.drawio             # Draw.io file for the data flow diagram
│   ├── data_models.drawio           # Draw.io file for data models (star schema)
│   └── naming-conventions.md        # Consistent naming guidelines for tables, columns, and files
├── scripts/                          # SQL scripts for ETL and transformations
│   ├── bronze/                      # Scripts for extracting and loading raw data
│   ├── silver/                      # Scripts for cleaning and transforming data
│   └── gold/                        # Scripts for creating analytical models
├── tests/                            # Test scripts and quality files
├── README.md                         # Project overview and instructions
├── LICENSE                           # License information for the repository
├── .gitignore                        # Files and directories to be ignored by Git
└── requirements.txt                  # Dependencies and requirements for the project



