# DATA 607 – Project 3: Google Trends Analysis  
**Group 1 – Team Repository**  
**Verified by Kevin Martin (CUNY SPS, Fall 2025)**  

---

## Project Overview
This project analyzes **Google Trends data** related to key **data science skills** — *Python, R, SQL, and Tableau*.  
The team’s objective is to track interest levels over time and load the data into a structured SQLite database for querying, visualization, and trend comparison.

The data was collected from Google Trends (via direct CSV export and the `gtrendsR` package) and processed into two formats:
- A **long-format CSV** for analysis and visualization.
- A **SQLite database** containing normalized tables for query and skill-level aggregation.

---

## Project Components
| Folder/File | Description |
|--------------|--------------|
| **`scripts/data607_project3.Rmd`** | Main R Markdown file developed by Joao — cleans and reshapes the Google Trends CSV, then writes to SQLite. |
| **`scripts/data607_project3.html`** | Rendered HTML output showing data pipeline execution and logs. |
| **`scripts/load_to_sqlite.R`** | Loader script for database testing and table validation (used by Kevin). |
| **`data/trends_long.csv`** | Long-format cleaned dataset (tidy version for analysis and visualization). |
| **`data/warehouse.db`** | SQLite database with structured tables (`Skill`, `TrendQuery`, `TrendPoint`). |
| **`README.md`** | Project summary and verification log. |

---

## Database Schema
The SQLite database contains three relational tables:

| Table | Description |
|--------|--------------|
| **Skill** | Contains unique skill names (e.g., Python, R, SQL, Tableau). |
| **TrendQuery** | Defines region, time window, and retrieval details for each skill. |
| **TrendPoint** | Stores weekly interest scores (date, interest, query ID). |

---

## Verification Notes
As of **October 17, 2025**:
- CSV (`trends_long.csv`) and SQLite DB (`warehouse.db`) successfully generated under `data/`.
- Verified database contains three tables: **Skill**, **TrendQuery**, **TrendPoint**.
- Confirmed database read/write operations in RStudio using:
  ```r
  library(DBI); library(RSQLite)
  con <- dbConnect(SQLite(), "data/warehouse.db")
  dbListTables(con)
  head(dbReadTable(con, "TrendPoint"))
  dbDisconnect(con)
  
  ---

### Kevin Martin — Code Validation Notebook

This section of the project focuses on validating the data transformation process and loading the cleaned dataset into SQLite for analysis.  
View the full R Markdown results on RPubs:  
 [DATA 607 Project 3 — Code Validation and SQLite Integration (Kevin Martin)](http://rpubs.com/Kevin_Martin16/1356951)

---

  
