#!/usr/bin/env python3
"""
PowerPoint Presentation Generator (`make_presentation.py`)
Generates `presentation/Capacity_Planning_Manpower_Spark.pptx` summarizing the
Apache Spark Manpower Capacity Planning & Stochastic FTE Optimization Pipeline.
"""
import os
import shutil
import sys

WORKSPACE_ROOT = "/working_dir/c_e74acd92e9b3463e"
SKILL_SCRIPTS = os.path.join(WORKSPACE_ROOT, "skills", "powerpoint-processing", "scripts")
if os.path.exists(SKILL_SCRIPTS) and SKILL_SCRIPTS not in sys.path:
    sys.path.insert(0, SKILL_SCRIPTS)


def build_with_deck_kit(output_pptx: str, img1_path: str, img2_path: str) -> None:
    from deck_kit import Deck, PALETTES, Theme

    theme = Theme(**PALETTES["navy"], alternate_tint_slides=True)
    deck = Deck(theme)

    # Slide 1: Cover
    deck.cover(
        style="editorial",
        dark=True,
        kicker="Distributed Analytics · IM60210",
        title="Manpower Capacity Planning via Apache Spark",
        subtitle="Distributed shift utilization tracking, fatigue modeling & stochastic FTE optimization",
        pill="Anjali (22IM30004) · Industrial & Systems Engineering, IIT Kharagpur",
        footer="IIT Kharagpur · Apache Spark Big Data Pipeline",
        notes=(
            "Welcome to this presentation on Big Data Analytics for Industrial Manpower Estimation "
            "and Shift Capacity Planning using Apache Spark. We examine how distributed DataFrame and "
            "RDD pipelines resolve shift bottlenecks across five manufacturing plants. "
            "Sources: README.md and Apache Spark Documentation (https://spark.apache.org/docs/latest/api/python/index.html)."
        ),
    )

    # Slide 2: KPI Row — Executive Overview & Empirical Scale
    deck.kpi_row(
        kicker="Executive Summary",
        title="Telemetry exposes $3.04M in shift capacity variance",
        subtitle="90-day distributed analysis across 5 plants, 8 roles, and 5 production departments",
        stats=[
            {
                "icon": "dataset",
                "value": "6,200",
                "label": "Shift telemetry logs",
                "detail": "Plus 3,600 daily workload rows",
            },
            {
                "icon": "warning",
                "value": "2,468",
                "label": "Overburdened shifts",
                "detail": "39.81% exceed 100% capacity",
            },
            {
                "icon": "payments",
                "value": "$3.04M",
                "label": "Gross inefficiency",
                "detail": "57.5k deficit & 65.7k idle hrs",
            },
            {
                "icon": "sync_alt",
                "value": "$1.13M",
                "label": "Reallocation savings",
                "detail": "36.98% float-pool recovery",
            },
        ],
        callout="Parallel shift deficits (57,467.43 hrs) and idle surpluses (65,742.71 hrs) confirm that static rosters misallocate headcount.",
        notes=(
            "Across 6,200 shift logs and 3,600 daily workload histories, 39.81 percent of shifts experience "
            "overtime or critical bottlenecks while 18.44 percent sit under-utilized. Dynamic cross-training recovers $1.13M. "
            "Sources: output/job3_cost_reallocation_summary.csv and generate_dataset.py."
        ),
    )

    # Slide 3: Split Image — Problem Context & Ergonomic Fatigue Decay
    deck.split_image(
        kicker="Operational Problem & Fatigue Modeling",
        title="Absenteeism and strain erode shift capacity",
        subtitle="Static rosters ignore wearable strain excursions and cycle-time volatility",
        image=img1_path,
        side="right",
        bullets=[
            "**Labor Share:** Accounts for 15% to 30% of total manufacturing cost.",
            "**Absenteeism:** 4.14% to 8.15% absence shrinks active shift coverage.",
            "**Fatigue Decay:** Excess strain cuts output by 20% per strain unit.",
        ],
        stats=[
            {"value": "15-30%", "label": "Labor cost share"},
            {"value": "0.20x", "label": "Decay per strain unit"},
        ],
        notes=(
            "Traditional spreadsheet planning assumes every rostered operator delivers 8 full productive hours. "
            "In reality, absenteeism, setup breaks, and physical fatigue degrade effective capacity and trigger costly overtime. "
            "Sources: Rework Manufacturing Workforce Guide (https://resources.rework.com/libraries/manufacturing-growth/manufacturing-workforce-planning) and jobs/job1_utilization_tracker/spark_job.py."
        ),
    )

    # Slide 4: Steps — End-to-End Apache Spark Pipeline Architecture
    deck.steps(
        kicker="Distributed Pipeline Architecture",
        title="Three-stage Apache Spark pipeline from telemetry to FTE policy",
        subtitle="In-memory DataFrame Catalyst optimization paired with modular RDD MapReduce stages",
        layout="timeline",
        steps=[
            {
                "label": "Ingest",
                "title": "MES & HRMS Logs",
                "body": "Loads 6,200 shift logs and 3,600 daily workload histories into Spark.",
            },
            {
                "label": "Stage A",
                "title": "Utilization Tracker",
                "body": "Computes fatigue decay F_decay, effective capacity, and 4 risk regimes.",
            },
            {
                "label": "Stage B",
                "title": "Stochastic FTE Policy",
                "body": "Sizes safety buffers under coupled variance at Z = 1.65 (95% SLA).",
            },
            {
                "label": "Stage C",
                "title": "Cost Reallocation",
                "body": "Quantifies overtime and idle losses, modeling 35-45% float recovery.",
            },
        ],
        aside={
            "icon": "bolt",
            "title": "Spark Engine Speed",
            "body": "Spark SQL Catalyst and 8 shuffle partitions process 9,800 rows in <0.25s.",
        },
        notes=(
            "Our architecture mirrors modular distributed analytics design using three sequential Apache Spark jobs. "
            "Each stage reads structured CSV partitions, applies vectorized SQL and RDD transformations, and writes curated outputs. "
            "Sources: run_pipeline.py and PySpark SQL Documentation (https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)."
        ),
    )

    # Slide 5: Split Image — Apache Spark Implementation & Mathematical Formulation
    deck.split_image(
        kicker="Algorithmic Formulation",
        title="PySpark models fatigue and stochastic variance",
        subtitle="Combining ergonomic productivity decay with coupled safety staffing equations",
        image=img2_path,
        side="left",
        bullets=[
            "**Fatigue Factor:** F_decay = 1.0 + 0.20 * max(0, E_avg - E_limit).",
            "**Coupled SD:** sigma_W = sqrt(tau^2 * sigma_D^2 + D^2 * sigma_tau^2).",
            "**Optimal FTE:** ceil((W_bar + 1.65 * sigma_W) / (7.0 * (1 - a_bar))).",
        ],
        stats=[
            {"value": "Z = 1.65", "label": "95% SLA target"},
            {"value": "7.0 hrs", "label": "Net shift hrs/FTE"},
        ],
        notes=(
            "Job 1 models productivity degradation from ergonomic strain, while Job 2 adapts the coupled stochastic "
            "variance equation to compute exact safety staffing buffers in Full-Time Equivalents. "
            "Sources: jobs/job1_utilization_tracker/spark_job.py, jobs/job2_headcount_optimization/spark_job.py, and Kickidler Workforce Formula (https://www.kickidler.com/info/workforce-capacity-planning)."
        ),
    )

    # Slide 6: Chart — Job 1 Shift Utilization Regime Distribution
    deck.chart(
        kicker="Job 1 Empirical Results",
        title="39.81% of shifts run in overtime or critical bottleneck regimes",
        subtitle="Shift counts by capacity status across 5 manufacturing departments (90-day horizon, N = 6,200)",
        chart={
            "type": "stacked",
            "categories": ["Machining", "Assembly", "Logistics", "Maintenance", "Quality Ctrl"],
            "series": [
                {"name": "Optimal Balanced (80-100%)", "values": [668, 661, 608, 341, 311]},
                {"name": "Overtime Warning (100-115%)", "values": [329, 307, 341, 178, 178]},
                {"name": "Understaffed Critical (>115%)", "values": [280, 273, 286, 145, 151]},
                {"name": "Surplus Idle (<80%)", "values": [271, 300, 278, 145, 149]},
            ],
            "number_format": "#,##0",
            "legend": True,
        },
        sidebar=[
            {"value": "2,589 shifts", "label": "Optimal Balanced (41.76%)"},
            {"value": "2,468 shifts", "label": "Overburdened (39.81%)"},
            {"value": "1,143 shifts", "label": "Surplus Idle (18.44%)"},
        ],
        footnote="N = 6,200 shifts across PLANT_01 to PLANT_05",
        notes=(
            "Job 1 reveals that while 2,589 shifts operate within the healthy 80 to 100 percent utilization band, "
            "2,468 shifts suffer from overtime or critical understaffing and 1,143 shifts have idle capacity. "
            "Sources: output/job1_utilization_risk.csv and output/job3_cost_reallocation_summary.csv."
        ),
    )

    # Slide 7: Table — Job 2 Stochastic Manpower & Safety Staffing Policy (PLANT_01)
    deck.table(
        kicker="Job 2 Empirical Results · PLANT_01",
        title="Stochastic safety buffers protect 95% shift SLA against volatility",
        subtitle="Optimized daily manpower policy across all 8 specialized operator roles at PLANT_01 (Z = 1.65)",
        columns=[
            "Role ID",
            "Avg Man-Hrs",
            "Absence %",
            "Base FTE",
            "Safety FTE",
            "Optimal FTE",
        ],
        rows=[
            ["PCB_ASSEMBLER", "174.82", "6.25%", "26.64", "8.15", "35 (Cap 39)"],
            ["PACKAGING_ASSOC", "150.88", "7.17%", "23.22", "7.19", "31 (Cap 35)"],
            ["WELDING_SPEC", "130.32", "8.15%", "20.27", "8.87", "30 (Cap 34)"],
            ["CNC_MACHINIST", "136.91", "6.78%", "20.98", "7.48", "29 (Cap 33)"],
            ["FORKLIFT_OP", "117.79", "7.91%", "18.27", "7.08", "26 (Cap 29)"],
            ["ROBOTICS_TECH", "105.62", "4.64%", "15.82", "5.20", "22 (Cap 25)"],
            ["QA_INSPECTOR", "93.73", "4.29%", "13.99", "5.07", "20 (Cap 23)"],
            ["PLC_MAINT_ENG", "77.63", "4.14%", "11.57", "4.07", "16 (Cap 18)"],
        ],
        highlight_col=5,
        footnote="Optimal FTE = ceil(Base FTE + Safety FTE); Cap = Peak Flex Cap (+15% base)",
        notes=(
            "This table presents the Job 2 stochastic headcount policy for PLANT_01. Roles with higher absenteeism "
            "and cycle-time variance, such as Welding Specialists (8.15% absence), require an 8.87 FTE safety buffer. "
            "Sources: output/job2_headcount_optimization.csv."
        ),
    )

    # Slide 8: Chart — Job 3 Gross Inefficiency Cost vs Cross-Training Savings
    deck.chart(
        kicker="Job 3 Financial Impact Analysis",
        title="Cross-trained float pools recover $1.13M across five departments",
        subtitle="Gross overtime & idle wage loss vs. cross-training reallocation savings ($ thousands)",
        chart={
            "type": "column",
            "categories": ["Machining", "Assembly", "Logistics", "Maintenance", "Quality Ctrl"],
            "series": [
                {"name": "Gross Inefficiency ($k)", "values": [861.83, 855.92, 629.62, 383.78, 313.20]},
                {"name": "Reallocation Savings ($k)", "values": [318.31, 317.22, 233.02, 141.06, 116.06]},
                {"name": "Net Residual Cost ($k)", "values": [543.52, 538.70, 396.60, 242.72, 197.13]},
            ],
            "number_format": '"$"#,##0.0"k"',
            "legend": True,
        },
        sidebar=[
            {"value": "$861.8k", "label": "Machining gross loss"},
            {"value": "$855.9k", "label": "Assembly gross loss"},
            {"value": "36.98%", "label": "Enterprise recovery"},
        ],
        footnote="Figures in USD thousands ($k) over 90-day multi-plant evaluation window",
        notes=(
            "Machining ($861.8k) and Assembly ($855.9k) account for the largest gross labor variance costs. "
            "Deploying cross-trained float pools across concurrent deficit and surplus shifts saves $1.13M enterprise-wide. "
            "Sources: output/job3_cost_reallocation_summary.csv."
        ),
    )

    # Slide 9: Comparison — Static Rostering vs Spark-Driven Dynamic Capacity Planning
    deck.comparison(
        kicker="Operational Transformation",
        title="Static rosters vs. Spark capacity optimization",
        subtitle="Comparing planning latency, fatigue fidelity, and cost resilience",
        left={
            "label": "Static Rostering",
            "tone": "negative",
            "icon": "cancel",
            "items": [
                "**Fixed Ratios:** Ignores order and cycle-time variance.",
                "**Fatigue Blind:** Misses wearable strain excursions.",
                "**Siloed Units:** 65.7k idle hrs alongside $3.04M loss.",
                "**Slow Updates:** Weekly sheets miss absence spikes.",
            ],
        },
        right={
            "label": "Spark Capacity Pipeline",
            "tone": "positive",
            "icon": "check_circle",
            "items": [
                "**Stochastic SLA:** Z = 1.65 buffers from joint variance.",
                "**Fatigue Model:** Scales capacity by F_decay factor.",
                "**Float Pooling:** Saves $1.13M (36.98% recovery).",
                "**Fast Engine:** Processes 9,800 rows in <0.25s.",
            ],
        },
        notes=(
            "Comparing legacy spreadsheet rostering against our Apache Spark pipeline highlights three core gains: "
            "biomathematical fatigue fidelity, coupled stochastic SLA buffers, and automated float-pool cost recovery. "
            "Sources: README.md, output/job2_headcount_optimization.csv, and output/job3_cost_reallocation_summary.csv."
        ),
    )

    # Slide 10: Closing — Implementation Roadmap
    deck.closing(
        style="next_steps",
        dark=True,
        kicker="Strategic Roadmap & Conclusion",
        title="Deploying Spark manpower optimization on plant MES streams",
        subtitle="Four-pillar rollout for real-time shift reallocation and SLA assurance",
        cards=[
            {
                "icon": "sensors",
                "title": "Structured Streaming",
                "body": "Connect Job 1 to live MQTT wearable strain and attendance feeds at shift start.",
            },
            {
                "icon": "groups",
                "title": "Cross-Training Matrix",
                "body": "Certify 20% of Assembly, Logistics, and Machining staff to unlock $1.13M savings.",
            },
            {
                "icon": "tune",
                "title": "Dynamic Roster Caps",
                "body": "Enforce Job 2 Optimal FTE and Peak Flex Caps across all 5 plants.",
            },
            {
                "icon": "analytics",
                "title": "Rolling Recalibration",
                "body": "Schedule nightly spark-submit jobs on YARN to update Z = 1.65 buffers.",
            },
        ],
        meta="Anjali (22IM30004) · Distributed Analytics (IM60210) · IIT Kharagpur",
        footer="Department of Industrial & Systems Engineering · IIT Kharagpur",
        notes=(
            "To conclude, integrating our three Apache Spark jobs with live attendance and MES streams transforms "
            "workforce capacity planning from a reactive cost center into a resilient, SLA-driven operation. "
            "Sources: README.md and output/job3_cost_reallocation_summary.csv."
        ),
    )

    # Slide 11: Sources (must be last slide per validator rules)
    deck.sources(
        title="Technical & Academic References",
        sources=[
            "Apache Spark Official Documentation — PySpark 4.3.0 SQL, DataFrames & RDD Programming Guide (https://spark.apache.org/docs/latest/api/python/index.html)",
            "Rework Manufacturing Growth Library — Manufacturing Workforce Planning: Strategic Staffing for Production (https://resources.rework.com/libraries/manufacturing-growth/manufacturing-workforce-planning)",
            "Kickidler Enterprise Guide — Workforce Capacity Planning: Process, Formula, and Absenteeism Adjustment (https://www.kickidler.com/info/workforce-capacity-planning)",
            "Reference Repository Structure — anmolsinghh614/inventory-optimization-perishables-hadoop (https://github.com/anmolsinghh614/inventory-optimization-perishables-hadoop)",
            "Project Datasets & Spark Outputs — data/raw_shift_telemetry.csv, data/daily_workload_history.csv, output/job1_utilization_risk.csv, output/job2_headcount_optimization.csv, output/job3_cost_reallocation_summary.csv",
        ],
        notes=(
            "All empirical metrics, equations, and architectural benchmarks in this presentation are grounded in the "
            "project datasets, Apache Spark documentation, and manufacturing workforce planning literature listed here. "
            "Sources: README.md and https://spark.apache.org/docs/latest/api/python/index.html."
        ),
    )

    os.makedirs(os.path.dirname(output_pptx), exist_ok=True)
    deck.save(output_pptx)


def build_fallback_pptx(output_pptx: str) -> None:
    from pptx import Presentation
    from pptx.util import Inches

    prs = Presentation()
    prs.slide_width = Inches(10.0)
    prs.slide_height = Inches(5.625)
    slides_data = [
        ("Manpower Capacity Planning via Apache Spark", "Anjali (22IM30004) | IM60210 | IIT Kharagpur"),
        ("Executive Summary", "6,200 shift logs | 3,600 daily workload logs | $3.04M gross variance | $1.13M recovered"),
        ("Ergonomic Fatigue & Absenteeism Model", "F_decay = 1.0 + 0.20 * delta_E | C_eff = Present_HC * Net_Hrs / F_decay"),
        ("Apache Spark 3-Job Architecture", "Job 1: Utilization Tracker -> Job 2: Stochastic FTE Optimizer -> Job 3: Cost Reallocation"),
        ("Job 1 Empirical Results", "2,589 Optimal | 2,468 Overburdened (39.81%) | 1,143 Surplus Idle (18.44%)"),
        ("Job 2 Stochastic FTE Policy (PLANT_01)", "Z = 1.65 (95% SLA) | PCB Assembler: 35 FTE | Packaging: 31 FTE | Welding: 30 FTE | CNC: 29 FTE"),
        ("Job 3 Financial Reallocation Impact", "Machining: $861.8k gross ($318.3k saved) | Assembly: $855.9k gross ($317.2k saved)"),
        ("Static Rostering vs Spark Optimization", "36.98% overall cost recovery ($1.13M saved) via cross-trained float pools"),
    ]
    for title_text, body_text in slides_data:
        slide = prs.slides.add_slide(prs.slide_layouts[1])
        slide.shapes.title.text = title_text
        slide.placeholders[1].text = body_text
    os.makedirs(os.path.dirname(output_pptx), exist_ok=True)
    prs.save(output_pptx)


if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    out_pptx = os.path.join(base, "presentation", "Capacity_Planning_Manpower_Spark.pptx")
    img1 = "/working_dir/c_e74acd92e9b3463e/artifacts/file_generation/ttl=63d/intermediate/smart_factory_floor.jpg"
    img2 = "/working_dir/c_e74acd92e9b3463e/artifacts/file_generation/ttl=63d/intermediate/spark_cluster_datacenter.jpg"
    try:
        import deck_kit  # noqa: F401
        build_with_deck_kit(out_pptx, img1, img2)
    except ImportError:
        build_fallback_pptx(out_pptx)
    out_copy = "/working_dir/c_e74acd92e9b3463e/artifacts/file_generation/ttl=63d/output/Capacity_Planning_Manpower_Spark.pptx"
    shutil.copyfile(out_pptx, out_copy)
    print(f"[SUCCESS] Generated presentation deck: {out_pptx} and copied to {out_copy}")
