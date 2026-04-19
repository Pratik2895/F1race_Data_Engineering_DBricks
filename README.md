# 🏎️ F1 Azure Databricks Lakehouse

> End-to-end **Formula 1 analytics lakehouse on Azure** — ADF orchestration, ADLS Gen2 storage, Delta Lake, Unity Catalog governance, and Power BI reporting. Demonstrates incremental + full-load patterns with enterprise-grade governance.

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD4?style=flat-square)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governance-FF3621?style=flat-square)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)

🔗 **Portfolio case study →** https://pratik2895.github.io/projects/f1-azure-databricks/

---

## 🎯 The problem

Formula 1 race data is rich but heavily normalized across many feeds — results, lap times, pit stops, constructor standings. Turning it into analyst-ready tables requires the full lakehouse pattern, not just raw storage.

## 🏗️ Architecture

```
 ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────┐
 │  Ergast  │──▶│   ADF    │──▶│   ADLS Gen2  │──▶│  Databricks  │──▶│ Power BI │
 │   API    │   │ pipelines│   │ raw/curated/ │   │   PySpark +  │   │ reports  │
 └──────────┘   └──────────┘   │   presented  │   │  Spark SQL   │   └──────────┘
                                └──────────────┘   │  + Unity Cat │
                                                   └──────────────┘
```

| Layer | Tool | Purpose |
|---|---|---|
| **Ingestion** | Azure Data Factory | Triggers Databricks notebooks, parameterized for incremental + full loads |
| **Storage** | ADLS Gen2 | Raw / cleansed / curated zones with Delta Lake |
| **Compute** | Azure Databricks | PySpark transformations, Spark SQL aggregates, schema enforcement |
| **Governance** | Unity Catalog | Row-level access, lineage, audit |
| **Serving** | Power BI | Race performance, driver stats, constructor standings dashboards |

---

## 📊 At a glance

| | |
|---|---|
| Cloud | **Azure** |
| Governance | **Unity Catalog** |
| Load modes | **Incremental + Full** |
| Medallion layers | Raw · Cleansed · Curated |

---

## 🚀 Run it

Prerequisites:
- An Azure subscription with ADLS Gen2, ADF, and Databricks workspaces provisioned
- Unity Catalog enabled on your Databricks workspace
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) authenticated

```bash
# 1. Clone
git clone https://github.com/Pratik2895/F1race_Data_Engineering_DBricks
cd F1race_Data_Engineering_DBricks

# 2. Import notebooks
databricks workspace import-dir ./notebooks /Workspace/f1-lakehouse

# 3. Deploy ADF pipeline JSON
# (use ADF UI or az datafactory CLI)

# 4. Trigger the master pipeline
# ADF UI → pl_f1_master → Trigger
```

---

## 🧭 Repo structure

```
.
├── notebooks/
│   ├── 01_ingest/       # Bronze — API → ADLS raw
│   ├── 02_transform/    # Silver — clean, typecast, enforce schema
│   ├── 03_aggregate/    # Gold — driver stats, constructor standings
│   └── utils/           # Shared helpers, config
├── adf/                 # Azure Data Factory pipeline JSON
├── sql/                 # DDLs, Unity Catalog grants
├── powerbi/             # .pbix files + screenshots
└── README.md
```

---

## 🧠 Why this project matters

Complements my AWS day-job by demonstrating the **parallel Azure stack** — ADF + Unity Catalog + Power BI. Proves I can move between clouds without losing the shape of a good pipeline.

---

## 📫 Contact

**Pratik Bhikadiya** · Data & Analytics Engineer
[Portfolio](https://pratik2895.github.io) · [LinkedIn](https://www.linkedin.com/in/pratikbhikadiya/) · bhikadi@uwindsor.ca
