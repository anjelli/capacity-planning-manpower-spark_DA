#!/usr/bin/env python3
"""
End-to-End Apache Spark Execution Pipeline (`run_pipeline.py`)
Orchestrates the 3 distributed Manpower Capacity Planning Spark jobs:
  - Job 1: Shift Utilization, Fatigue Decay & Capacity Risk Tracker
  - Job 2: Stochastic Manpower & Safety Staffing (FTE) Optimization
  - Job 3: Overtime Cost & Cross-Training Redeployment Aggregator
"""
import os
import subprocess
import sys
import time


def run_stage(job_name: str, script_path: str, input_csv: str, output_csv: str) -> None:
    print(f"======================================================================")
    print(f"[SPARK PIPELINE] Starting {job_name}...")
    print(f"  Input : {input_csv}")
    print(f"  Output: {output_csv}")
    t0 = time.time()
    subprocess.run([sys.executable, script_path, input_csv, output_csv], check=True)
    elapsed = time.time() - t0
    with open(output_csv, "r") as f:
        row_count = sum(1 for _ in f) - 1
    print(f"[SUCCESS] Completed {job_name} in {elapsed:.2f}s ({row_count} output records).")


def main() -> None:
    base = os.path.dirname(os.path.abspath(__file__))
    shift_csv = os.path.join(base, "data", "raw_shift_telemetry.csv")
    workload_csv = os.path.join(base, "data", "daily_workload_history.csv")

    if not os.path.exists(shift_csv) or not os.path.exists(workload_csv):
        print("[INFO] Datasets not found. Running generate_dataset.py first...")
        subprocess.run([sys.executable, os.path.join(base, "generate_dataset.py")], check=True)

    jobs = [
        (
            "Job 1: Shift Utilization & Fatigue Decay Tracker",
            os.path.join(base, "jobs", "job1_utilization_tracker", "spark_job.py"),
            shift_csv,
            os.path.join(base, "output", "job1_utilization_risk.csv"),
        ),
        (
            "Job 2: Stochastic Manpower & Safety Staffing (FTE) Optimization",
            os.path.join(base, "jobs", "job2_headcount_optimization", "spark_job.py"),
            workload_csv,
            os.path.join(base, "output", "job2_headcount_optimization.csv"),
        ),
        (
            "Job 3: Overtime Cost & Cross-Training Redeployment Summary",
            os.path.join(base, "jobs", "job3_cost_reallocation_summary", "spark_job.py"),
            shift_csv,
            os.path.join(base, "output", "job3_cost_reallocation_summary.csv"),
        ),
    ]

    for job_name, script_path, in_csv, out_csv in jobs:
        run_stage(job_name, script_path, in_csv, out_csv)

    print("======================================================================")
    print("[PIPELINE COMPLETE] All 3 Apache Spark jobs executed successfully.")


if __name__ == "__main__":
    main()
