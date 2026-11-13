# Big Data Analytics for Industrial Manpower Estimation & Shift Capacity Planning using Apache Spark

**Author:** Anjali (22IM30004)

**Course:** Distributed Analytics for Industrial Automation (IM60210)

**Department:** Industrial & Systems Engineering, IIT Kharagpur

## Executive Summary

Multi-plant manufacturing enterprises suffer substantial productivity losses, overtime cost overruns, and SLA fulfillment bottlenecks due to static shift rostering, unscheduled absenteeism, and ergonomic fatigue degradation ([Manufacturing Workforce Planning Guide](https://resources.rework.com/libraries/manufacturing-growth/manufacturing-workforce-planning)). This project implements a **Distributed Apache Spark Pipeline** (using [PySpark DataFrames, Spark SQL, and RDD Transformations](https://spark.apache.org/docs/latest/api/python/index.html)) to process enterprise-scale Manufacturing Execution System (MES) shift telemetry and historical production workload logs.

The pipeline executes three sequential Apache Spark jobs:

1. **Shift Utilization, Fatigue Decay & Capacity Risk Tracker:** Processes MES shift logs and wearable ergonomic strain telemetry to adjust effective labor capacity for fatigue-induced productivity decay and flags shifts experiencing critical bottlenecks or idle surplus.

2. **Stochastic Manpower & Safety Staffing (FTE) Optimization:** Computes Base Required Headcount ($ext{FTE}_{	ext{base}}$), Safety Staffing Buffer ($ext{FTE}_{	ext{safety}}$), and Optimal Rostered Headcount ($ext{FTE}_{	ext{optimal}}$) using coupled demand and cycle-time variance under target service levels ($Z = 1.65$ for 95% shift SLA coverage).

3. **Overtime Cost & Cross-Training Redeployment Aggregator:** Quantifies gross financial inefficiency from overtime premiums, SLA delay penalties, and unutilized idle wages, and estimates net labor savings achieved via cross-trained float-pool reallocation.

## Key Features & Business Impact

* **Ergonomic Fatigue Decay Modeling:** Dynamically adjusts effective shift man-hours based on ergonomic strain excursions ($F_{	ext{decay}} = 1.0 + 0.20 	imes \Delta E$) rather than assuming static nominal output ([Workforce Capacity Planning Process & Formula](https://www.kickidler.com/info/workforce-capacity-planning)).

* **Stochastic Safety Staffing Policy:** Formulates dynamic $(	ext{FTE}_{	ext{optimal}}, 	ext{FTE}_{	ext{max}})$ staffing policies accounting for coupled order volume variability ($\sigma_D$), unit cycle-time variance ($\sigma_	au$), and role-specific absenteeism ($ar{a}$).

* **Cross-Training Float Pool Recovery:** Identifies parallel shift deficits and idle surpluses across departments, enabling inter-shift operator reallocation that recovers **36.98% (\$1.13M)** of gross labor variance costs.

* **Production-Ready PySpark Architecture:** Implements distributed [PySpark SQL DataFrame transformations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) alongside modular RDD Map/Reduce stage scripts compatible with `spark-submit`, YARN, Amazon EMR, Databricks, and local execution pipelines.

## Data Architecture & Schema

### 1. `raw_shift_telemetry.csv` (MES Shift Execution & Wearable Ergonomic Logs)

* `shift_id`: Unique shift batch identifier (e.g., `SHF-10001` to `SHF-16200`)

* `plant_id`: Manufacturing facility identifier (`PLANT_01` to `PLANT_05`)

* `role_id`: Specialized operator role SKU (`CNC_MACHINIST`, `ROBOTICS_TECH`, `WELDING_SPEC`, `PCB_ASSEMBLER`, `QA_INSPECTOR`, `FORKLIFT_OP`, `PACKAGING_ASSOC`, `PLC_MAINT_ENG`)

* `department`: Production department (`Machining`, `Assembly`, `Quality_Control`, `Logistics`, `Maintenance`)

* `scheduled_headcount`: Rostered operators scheduled for the shift

* `present_headcount`: Operators physically present after unscheduled absenteeism

* `shift_duration_hrs`: Standard shift duration (`8.0` hours)

* `non_productive_hrs`: Setup, changeover, safety briefing, and statutory break hours per operator

* `hourly_wage_usd`: Standard hourly base wage (\$/hr)

* `overtime_rate_usd`: Overtime premium hourly rate (\$/hr, $1.5	imes$ base wage)

* `shift_date`: Shift execution date (`YYYY-MM-DD`)

* `assigned_workload_hrs`: Total standard man-hours of production work assigned to the shift

* `ergonomic_strain_index`: Average wearable/MES ergonomic strain index recorded during the shift

* `target_strain_index`: Baseline sustainable ergonomic strain threshold

* `strain_tolerance`: Allowable positive strain deviation before fatigue decay occurs

### 2. `daily_workload_history.csv` (Historical Production Demand & Cycle-Time Logs)

* `plant_id`: Manufacturing facility identifier (`PLANT_01` to `PLANT_05`)

* `role_id`: Specialized operator role SKU

* `date`: Production date (`YYYY-MM-DD`, 90-day horizon)

* `daily_order_units`: Daily production units demanded

* `unit_cycle_time_hrs`: Observed labor cycle time per unit (man-hours/unit)

* `daily_required_manhours`: Total daily labor demand ($W_t = 	ext{daily\_order\_units} 	imes 	ext{unit\_cycle\_time\_hrs}$)

* `absenteeism_rate`: Daily fractional absenteeism rate observed for the role

## Apache Spark Jobs Breakdown

### Job 1: Shift Utilization, Fatigue Decay & Capacity Risk Tracker (`jobs/job1_utilization_tracker`)

* **Spark Transformation (`spark_job.py` / `mapper.py`):** Parses raw shift telemetry, evaluates ergonomic strain excess ($\Delta E = \max(0, E_{	ext{avg}} - (E_{	ext{target}} + E_{	ext{tol}}))$), computes Fatigue Decay Factor ($F_{	ext{decay}} = 1.0 + 0.20 \cdot \Delta E$), determines Effective Shift Capacity ($C_{	ext{eff}} = 	ext{present\_hc} 	imes (	ext{shift\_hrs} - 	ext{non\_prod\_hrs}) / F_{	ext{decay}}$), calculates Effective Utilization ($ho = W_{	ext{assigned}} / C_{	ext{eff}}$), and assigns capacity risk status (`UNDERSTAFFED_CRITICAL`, `OVERTIME_WARNING`, `OPTIMAL_BALANCED`, `SURPLUS_IDLE`).

* **Spark Aggregation (`spark_job.py` / `reducer.py`):** Aggregates shift counts, average scheduled/present headcount, total workload hours, effective capacity hours, deficit/surplus hours, and mean utilization percentage per Plant, Role, Department, and Status.

### Job 2: Stochastic Manpower & Safety Staffing Optimization (`jobs/job2_headcount_optimization`)

* **Spark Transformation (`spark_job.py` / `mapper.py`):** Partitions daily production demand, unit cycle times, required man-hours, and absenteeism rates by `(plant_id, role_id)`.

* **Spark Aggregation (`spark_job.py` / `reducer.py`):** Computes mean and standard deviation of daily order demand ($ar{D}, \sigma_D$), unit cycle time ($ar{	au}, \sigma_	au$), coupled workload standard deviation ($\sigma_W = \sqrt{ar{	au}^2 \sigma_D^2 + ar{D}^2 \sigma_	au^2}$), Net Productive Hours per FTE ($H_{	ext{net}} = 7.0 	imes (1 - ar{a})$), Base FTE (\$	ext{FTE}*{	ext{base}} = ar{W} / H*{	ext{net}}$), Safety Staffing Buffer ($ext{FTE}_{	ext{safety}} = Z \cdot \sigma_W / H_{	ext{net}}$ at $Z = 1.65$), Optimal Rostered FTE ($\\lceil 	ext{FTE}*{	ext{base}} + 	ext{FTE}*{	ext{safety}} ceil\$), and Peak Flex Cap FTE.

### Job 3: Overtime Cost & Cross-Training Redeployment Summary (`jobs/job3_cost_reallocation_summary`)

* **Spark Transformation (`spark_job.py` / `mapper.py`):** Computes gross financial loss from overtime premiums and SLA penalties on overburdened shifts ($ho > 1.00$) and unutilized wage costs on surplus idle shifts ($ho < 0.80$), modeling 35%–45% cost recovery via cross-trained float pools.

* **Spark Aggregation (`spark_job.py` / `reducer.py`):** Aggregates overburdened shifts, idle shifts, total deficit/surplus hours, gross inefficiency cost ($), cross-training reallocation savings ($), and net residual labor variance cost (\$) by department.

## Repository Structure

```
.
├── .gitignore                                 <- Git ignore rules for Python and Spark artifacts
├── README.md                                  <- Comprehensive project documentation
├── generate_dataset.py                        <- Generates realistic MES shift logs & workload history
├── run_pipeline.py                            <- End-to-end Apache Spark execution pipeline
├── visualizations.py                          <- Generates publication-ready figures from Spark outputs
├── make_presentation.py                       <- Generates the PowerPoint presentation deck
├── jobs/
│   ├── job1_utilization_tracker/
│   │   ├── spark_job.py                       <- PySpark DataFrame/SQL shift utilization & fatigue job
│   │   ├── mapper.py                          <- Spark RDD / partition map transformation stage
│   │   └── reducer.py                         <- Spark RDD / partition reduce aggregation stage
│   ├── job2_headcount_optimization/
│   │   ├── spark_job.py                       <- PySpark DataFrame/SQL stochastic FTE optimization job
│   │   ├── mapper.py                          <- Daily demand, cycle-time & absenteeism mapper
│   │   └── reducer.py                         <- Coupled variance & safety staffing buffer reducer
│   └── job3_cost_reallocation_summary/
│       ├── spark_job.py                       <- PySpark DataFrame/SQL cost & reallocation summary job
│       ├── mapper.py                          <- Overtime penalty & idle wage inefficiency mapper
│       └── reducer.py                         <- Departmental cost & cross-training savings reducer
├── data/
│   ├── raw_shift_telemetry.csv                <- 6,200 shift execution records across 5 plants
│   └── daily_workload_history.csv             <- 90 days of plant-role demand & cycle-time history
├── output/
│   ├── job1_utilization_risk.csv              <- Plant-role shift utilization and capacity risks
│   ├── job2_headcount_optimization.csv        <- Optimized base FTE, safety buffer & roster caps
│   └── job3_cost_reallocation_summary.csv     <- Departmental overtime loss & reallocation metrics
└── presentation/
    ├── Capacity_Planning_Manpower_Spark.pptx  <- Complete presentation deck
    └── figures/
        ├── utilization_by_department.png      <- Shift utilization regime breakdown chart
        ├── labor_cost_reallocation.png        <- Gross inefficiency vs cross-training recovery chart
        └── headcount_policy_fte_buffer.png    <- Stochastic FTE staffing policy chart

```

## Summary of Empirical Findings

| Department | Total Shifts | Overburdened Shifts | Surplus Idle Shifts | Deficit Hours | Surplus Hours | Gross Inefficiency Cost (\$) | Reallocation Savings (\$) | Net Residual Cost (\$) | Recovery Rate (%) | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **Machining** | 1,548 | 609 | 271 | 15,360.89 | 17,015.69 | \$861,828.33 | \$318,311.22 | \$543,517.12 | 36.93% | 
| **Assembly** | 1,541 | 580 | 300 | 15,492.81 | 19,214.59 | \$855,922.07 | \$317,224.19 | \$538,697.88 | 37.06% | 
| **Logistics** | 1,513 | 627 | 278 | 15,253.96 | 17,339.02 | \$629,620.23 | \$233,016.52 | \$396,603.71 | 37.01% | 
| **Maintenance** | 809 | 323 | 145 | 5,351.03 | 5,546.49 | \$383,778.62 | \$141,062.86 | \$242,715.76 | 36.76% | 
| **Quality Control** | 789 | 329 | 149 | 6,008.74 | 6,626.92 | \$313,197.49 | \$116,062.64 | \$197,134.84 | 37.06% | 
| **Enterprise Total** | **6,200** | **2,468** | **1,143** | **57,467.43** | **65,742.71** | **\$3,044,346.74** | **\$1,125,677.43** | **\$1,918,669.31** | **36.98%** | 

## Execution Instructions

### 1. Generate Synthetic Datasets

```
python generate_dataset.py

```

### 2. Run Apache Spark Pipeline (Local PySpark or Cluster `spark-submit`)

```
# End-to-end automated pipeline execution
python run_pipeline.py

# Direct Apache Spark Cluster Submission (e.g., Job 1, Job 2, Job 3)
spark-submit --master yarn --deploy-mode client \
  jobs/job1_utilization_tracker/spark_job.py \
  data/raw_shift_telemetry.csv \
  output/job1_utilization_risk.csv

spark-submit --master yarn --deploy-mode client \
  jobs/job2_headcount_optimization/spark_job.py \
  data/daily_workload_history.csv \
  output/job2_headcount_optimization.csv

spark-submit --master yarn --deploy-mode client \
  jobs/job3_cost_reallocation_summary/spark_job.py \
  data/raw_shift_telemetry.csv \
  output/job3_cost_reallocation_summary.csv

```

### 3. Generate Analytical Visualizations

```
python visualizations.py

```

### 4. Generate PowerPoint Deck

```
python make_presentation.py

```

## References

1. [Apache Spark — PySpark 4.3.0 Overview & SQL DataFrame API Documentation](https://spark.apache.org/docs/latest/api/python/index.html)

2. [Rework Manufacturing Library — Manufacturing Workforce Planning: Strategic Staffing for Production](https://resources.rework.com/libraries/manufacturing-growth/manufacturing-workforce-planning)

3. [Kickidler — Workforce Capacity Planning: Process, Formula, and Template](https://www.kickidler.com/info/workforce-capacity-planning)

4. [Reference Architecture Repository — anmolsinghh614/inventory-optimization-perishables-hadoop](https://github.com/anmolsinghh614/inventory-optimization-perishables-hadoop)