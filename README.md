# Industrial Manpower Estimation & Shift Capacity Planning using Apache Spark

### A Distributed Analytics Framework for Workforce Planning, Labour Capacity Assessment and Operational Cost Analysis

**Author:** Anjali  
**Department:** Industrial and Systems Engineering, IIT KGP
**Course:** Distributed Analytics for Industrial Automation (IM60210)

---

## 1. Project Overview

Effective manpower planning is a critical component of industrial operations. Manufacturing facilities must continuously balance production workload, workforce availability, operator productivity, shift requirements and labour costs. Static workforce allocation can lead to understaffed shifts, excessive overtime, underutilized personnel and production-capacity bottlenecks.

This project develops a **distributed analytics framework using Apache Spark for industrial manpower estimation and shift-level capacity planning**. It integrates historical production workload, operator availability, absenteeism, shift utilization and ergonomic indicators to evaluate workforce requirements across multiple manufacturing facilities.

The framework processes manufacturing execution and production workload data using PySpark DataFrames, Spark SQL and distributed data transformations. It translates operational data into actionable workforce metrics, including required Full-Time Equivalent (FTE), staffing buffers, capacity utilization, workforce deficits and labour-cost indicators.

The proposed solution is structured around three analytical modules:

1. **Shift Utilization and Capacity Risk Analysis:** Evaluates available workforce capacity against assigned production workload and identifies understaffed, balanced and underutilized shifts.
2. **Stochastic Manpower Estimation:** Calculates base workforce requirements and additional staffing buffers by incorporating production-demand variability, processing-time fluctuations and absenteeism.
3. **Labour Cost Analysis and Workforce Redeployment:** Quantifies overtime and idle-capacity costs and evaluates potential benefits from deploying cross-trained personnel across compatible workforce requirements.

The overall objective is to support data-driven workforce decisions through scalable distributed analytics.

---

## 2. Project Objectives

The project focuses on the following industrial workforce planning objectives:

- **Manpower Estimation:** Determine workforce requirements from production workload and available productive working hours.
- **Capacity Planning:** Measure the relationship between scheduled manpower, actual workforce availability and assigned workload.
- **Demand Variability Assessment:** Incorporate fluctuations in production orders and unit processing times into staffing calculations.
- **Absenteeism Management:** Account for workforce availability variations when estimating roster requirements.
- **Shift-Level Utilization:** Identify capacity shortages, overtime pressure, balanced operations and surplus workforce capacity.
- **Labour Cost Analysis:** Examine overtime expenditure, idle workforce costs and potential workforce reallocation benefits.
- **Distributed Data Processing:** Use Apache Spark to process, transform and aggregate manufacturing workforce data across plants, departments and specialized operator roles.

---

## 3. System Architecture

The framework follows a modular, three-stage Apache Spark processing architecture.

### Job 1: Shift Utilization, Fatigue Decay and Capacity Risk Tracker

**Objective:** Evaluate workforce utilization and identify shift-level capacity imbalances.

This module analyses Manufacturing Execution System (MES) shift telemetry and ergonomic indicators to calculate effective workforce capacity.

Key analytical operations include:

- Processing scheduled and present operator headcount.
- Accounting for absenteeism and non-productive shift hours.
- Evaluating ergonomic strain relative to sustainable thresholds.
- Estimating fatigue-adjusted productive capacity.
- Comparing assigned workload against available labour capacity.
- Classifying shifts based on utilization and capacity risk.

The module calculates:

\[
C_{\text{eff}} =
\frac{
H_{\text{present}}\times
(T_{\text{shift}}-T_{\text{nonproductive}})
}{
F_{\text{decay}}
}
\]

Where:

- \(C_{\text{eff}}\) = effective labour capacity in man-hours.
- \(H_{\text{present}}\) = number of available operators.
- \(T_{\text{shift}}\) = scheduled shift duration.
- \(T_{\text{nonproductive}}\) = non-productive time per operator.
- \(F_{\text{decay}}\) = fatigue adjustment factor.

The utilization indicator is:

\[
\rho = \frac{W_{\text{assigned}}}{C_{\text{eff}}}
\]

Capacity conditions are classified into four operational categories:

| Capacity Status | Utilization |
|---|---:|
| Surplus Idle | Below 80% |
| Optimal Balanced | 80–100% |
| Overtime Warning | 100–115% |
| Understaffed Critical | Above 115% |

**Output:** Plant-, department- and role-level shift utilization and capacity-risk summaries.

### Job 2: Stochastic Manpower and Safety Staffing Optimization

**Objective:** Estimate the number of operators required to satisfy production workload under demand and processing-time uncertainty.

This module uses historical daily production workload data to estimate workforce requirements for each plant and specialized operator role.

The analysis incorporates:

- Average production demand.
- Demand standard deviation.
- Average unit cycle time.
- Cycle-time variability.
- Daily labour workload.
- Role-specific absenteeism.
- Target staffing service levels.

Daily production workload is calculated as:

\[
W_t = D_t \times \tau_t
\]

Where:

- \(D_t\) = production demand in units.
- \(\tau_t\) = labour cycle time per unit.
- \(W_t\) = required labour hours.

The framework estimates average workload variability using:

\[
\sigma_W =
\sqrt{
\bar{\tau}^{\,2}\sigma_D^2+
\bar D^{\,2}\sigma_\tau^2
}
\]

This provides a first-order approximation of workload uncertainty.

Net productive hours per operator are estimated as:

\[
H_{\text{net}}=7.0(1-\bar a)
\]

Where \(\bar a\) represents average absenteeism.

Base workforce requirement:

\[
FTE_{\text{base}} =
\frac{\bar W}{H_{\text{net}}}
\]

Safety staffing buffer:

\[
FTE_{\text{safety}} =
\frac{Z\sigma_W}{H_{\text{net}}}
\]

The rostered requirement is:

\[
FTE_{\text{optimal}} =
\left\lceil
FTE_{\text{base}}+FTE_{\text{safety}}
\right\rceil
\]

The model uses \(Z=1.65\), corresponding approximately to a one-sided 95% normal-distribution staffing coverage target under the model assumptions.

A peak workforce flexibility allowance is also calculated to support additional capacity planning.

**Output:** Plant- and role-level base FTE requirements, safety staffing buffers, optimal rostered headcount and flexible workforce capacity estimates.

### Job 3: Overtime Cost and Cross-Training Redeployment Analysis

**Objective:** Evaluate labour-cost inefficiencies and identify opportunities for more effective workforce utilization.

This module analyses operational shift data to quantify:

- Overtime-related labour costs.
- Production workload deficits.
- Unutilized workforce capacity.
- Idle labour expenditure.
- Department-level capacity imbalances.
- Potential savings from cross-training and workforce redeployment.

The analysis identifies overburdened shifts and shifts with surplus capacity. It then evaluates a cross-training float-pool recovery scenario to estimate the potential financial benefits of reallocating compatible workforce capacity.

The cost analysis aggregates:

\[
C_{\text{gross}} =
C_{\text{overtime}}+
C_{\text{idle}}+
C_{\text{SLA}}
\]

Potential workforce reallocation savings are evaluated using a predefined recovery assumption.

\[
C_{\text{net}} =
C_{\text{gross}}-
C_{\text{reallocation savings}}
\]

**Output:** Department-level labour-cost summaries, overtime and idle-capacity indicators, estimated redeployment savings and residual labour variance costs.

---

## 4. Technology Stack

| Technology | Application |
|---|---|
| Python | Core implementation and pipeline orchestration |
| Apache Spark | Distributed data processing |
| PySpark DataFrames | Structured data transformations |
| Spark SQL | Analytical queries and aggregations |
| Spark RDD | MapReduce-style distributed processing |
| Pandas | Output processing and reporting |
| NumPy | Numerical calculations |
| Matplotlib | Analytical visualizations |
| CSV | Structured input and output datasets |

The pipeline is designed for local PySpark execution and supports Spark cluster deployment configurations such as YARN, Amazon EMR and Databricks.

---

## 5. Dataset Description

The project uses synthetically generated manufacturing workforce datasets to create a reproducible multi-plant capacity-planning environment.

### Dataset 1: `raw_shift_telemetry.csv`

**Purpose:** Shift-level workforce utilization and capacity analysis.

| Attribute | Description |
|---|---|
| shift_id | Unique shift identifier |
| plant_id | Manufacturing facility identifier |
| role_id | Specialized operator role |
| department | Manufacturing department |
| scheduled_headcount | Number of scheduled operators |
| present_headcount | Actual available operators |
| shift_duration_hrs | Total shift duration |
| non_productive_hrs | Non-productive shift hours |
| hourly_wage_usd | Standard hourly wage |
| overtime_rate_usd | Overtime hourly rate |
| shift_date | Date of shift execution |
| assigned_workload_hrs | Total assigned production workload |
| ergonomic_strain_index | Average ergonomic strain |
| target_strain_index | Sustainable strain baseline |
| strain_tolerance | Permissible strain deviation |

**Dataset scale:**

- 6,200 shift execution records.
- Five manufacturing plants.
- Eight specialized operator roles.
- Five production and support departments.
- A 90-day operating horizon.

### Dataset 2: `daily_workload_history.csv`

**Purpose:** Historical workload variability and manpower estimation.

| Attribute | Description |
|---|---|
| plant_id | Manufacturing facility |
| role_id | Specialized operator role |
| date | Production date |
| daily_order_units | Daily production demand |
| unit_cycle_time_hrs | Labour cycle time per unit |
| daily_required_manhours | Total daily workload |
| absenteeism_rate | Daily role-level absenteeism |

**Dataset scale:**

- 3,600 daily workload observations.
- Five manufacturing plants.
- Eight specialized operator roles.
- Ninety days of production history.

The synthetic datasets provide a controlled experimental environment for examining workforce planning methods across different operating conditions.

---

## 6. Key Analytical Outputs

The framework generates three primary analytical output files.

| Output File | Analytical Purpose |
|---|---|
| `job1_utilization_risk.csv` | Shift utilization, effective capacity and capacity-risk classification |
| `job2_headcount_optimization.csv` | Base FTE, safety staffing and optimal rostered workforce |
| `job3_cost_reallocation_summary.csv` | Overtime expenditure, idle-capacity costs and redeployment scenarios |

### Illustrative Project Findings

The generated analytical results provide a department-level view of workforce utilization and labour-cost indicators.

The current synthetic-data scenario reports:

| Indicator | Scenario Result |
|---|---:|
| Total shift observations | 6,200 |
| Overburdened shifts | 2,468 |
| Surplus idle shifts | 1,143 |
| Gross labour inefficiency cost | $3,044,346.74 |
| Estimated reallocation savings | $1,125,677.43 |
| Net residual labour variance cost | $1,918,669.31 |
| Estimated recovery rate | 36.98% |

The results illustrate how shift telemetry and workforce analytics can be used to identify labour-capacity imbalances and evaluate potential workforce redeployment strategies.

These values are generated from the project's synthetic datasets and scenario assumptions.

---

## 7. Analytical Visualizations

The project generates visual summaries to communicate workforce capacity and labour-cost indicators.

### 1. Shift Utilization by Department

Displays the distribution of balanced, overburdened and underutilized shifts across manufacturing departments.

### 2. Labour Cost and Redeployment Analysis

Compares gross labour inefficiency costs, potential workforce redeployment savings and residual costs.

### 3. Stochastic Manpower Estimation

Visualizes base workforce requirements, safety staffing buffers and flexible workforce allowances by operator role.

Visualizations are generated using Matplotlib and saved under `presentation/figures/`.

---

## 8. Repository Structure

```text
capacity-planning-manpower-spark_DA/
│
├── README.md
├── .gitignore
│
├── generate_dataset.py
├── run_pipeline.py
├── visualizations.py
├── make_presentation.py
│
├── data/
│   ├── raw_shift_telemetry.csv
│   └── daily_workload_history.csv
│
├── jobs/
│   ├── job1_utilization_tracker/
│   │   ├── spark_job.py
│   │   ├── mapper.py
│   │   └── reducer.py
│   │
│   ├── job2_headcount_optimization/
│   │   ├── spark_job.py
│   │   ├── mapper.py
│   │   └── reducer.py
│   │
│   └── job3_cost_reallocation_summary/
│       ├── spark_job.py
│       ├── mapper.py
│       └── reducer.py
│
├── output/
│   ├── job1_utilization_risk.csv
│   ├── job2_headcount_optimization.csv
│   └── job3_cost_reallocation_summary.csv
│
└── presentation/
    ├── Capacity_Planning_Manpower_Spark.pptx
    └── figures/
        ├── utilization_by_department.png
        ├── labor_cost_reallocation.png
        └── headcount_policy_fte_buffer.png
```

---

## 9. Execution Instructions

### Prerequisites

- Python 3.9+
- Java runtime compatible with the selected Spark version
- Apache Spark / PySpark
- Required Python libraries

Install the dependencies:

```bash
pip install pyspark pandas numpy matplotlib
```

### Step 1: Generate the datasets

```bash
python generate_dataset.py
```

This creates the manufacturing shift telemetry and historical workload datasets.

### Step 2: Execute the complete analytics pipeline

```bash
python run_pipeline.py
```

The pipeline orchestrates the three Spark analytical jobs sequentially and generates their corresponding output datasets.

### Step 3: Generate analytical visualizations

```bash
python visualizations.py
```

The resulting figures are saved to:

```text
presentation/figures/
```

### Step 4: Generate the project presentation

```bash
python make_presentation.py
```

The presentation is saved under:

```text
presentation/
```

### Direct Spark Submission

Individual Spark jobs can also be submitted independently to a configured Spark cluster.

Example:

```bash
spark-submit --master yarn --deploy-mode client \
  jobs/job1_utilization_tracker/spark_job.py \
  data/raw_shift_telemetry.csv \
  output/job1_utilization_risk.csv
```

The same submission structure applies to the other analytical modules.

---

## 10. Business Applications

The framework demonstrates applications of distributed analytics in industrial workforce management:

- **Manufacturing Workforce Planning:** Estimate manpower requirements across plants and specialized operator categories.
- **Shift Capacity Management:** Identify bottlenecks and surplus workforce capacity.
- **Production Planning:** Relate production workload to available productive labour hours.
- **Absenteeism Planning:** Understand the impact of workforce availability on staffing requirements.
- **Labour Cost Management:** Evaluate overtime exposure and idle workforce expenditure.
- **Workforce Flexibility:** Examine cross-training and redeployment opportunities.
- **Operational Decision Support:** Provide management with quantitative workforce and capacity indicators.

The modular architecture can be extended to larger manufacturing datasets and additional workforce-planning scenarios.

---

## 11. Conclusion

This project demonstrates how Apache Spark and distributed analytics can be applied to industrial manpower estimation and shift capacity planning.

By integrating shift telemetry, production workload history, operator availability, demand variability and labour-cost indicators, the framework provides a structured analytical approach to workforce capacity assessment.

Its three-module architecture connects operational utilization analysis, stochastic FTE estimation and workforce redeployment assessment into a unified pipeline.

The project brings together concepts from **Industrial Engineering, Workforce Planning, Stochastic Modelling, Big Data Analytics and Distributed Computing**, demonstrating the role of data-driven methods in improving manufacturing resource planning and operational efficiency.

---

## 12. References

1. [Apache Spark Documentation – Python API](https://spark.apache.org/docs/latest/api/python/index.html)
2. [Apache Spark – PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
3. [Manufacturing Workforce Planning – Rework Resources](https://resources.rework.com/libraries/manufacturing-growth/manufacturing-workforce-planning)
4. [Workforce Capacity Planning – Kickidler](https://www.kickidler.com/info/workforce-capacity-planning)

---

**Project Domain:** Industrial Engineering | Manpower Estimation | Capacity Planning | Big Data Analytics | Apache Spark
