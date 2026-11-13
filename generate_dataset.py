#!/usr/bin/env python3
"""
Synthetic Dataset Generator for Industrial Manpower Capacity Planning (Apache Spark)
Generates:
  1. data/raw_shift_telemetry.csv (6,200 shift execution & wearable/MES strain logs across 5 plants)
  2. data/daily_workload_history.csv (90 days of production demand, cycle time & absenteeism history)
"""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

PLANTS = ["PLANT_01", "PLANT_02", "PLANT_03", "PLANT_04", "PLANT_05"]

ROLES = [
    {
        "role_id": "CNC_MACHINIST",
        "department": "Machining",
        "base_roster": 18,
        "hourly_wage": 28.50,
        "overtime_rate": 42.75,
        "target_strain": 4.0,
        "strain_tol": 1.2,
        "base_orders": 210,
        "order_sd": 34,
        "cycle_time": 0.58,
        "cycle_sd": 0.06,
        "base_abs": 0.065,
    },
    {
        "role_id": "ROBOTICS_TECH",
        "department": "Assembly",
        "base_roster": 14,
        "hourly_wage": 32.00,
        "overtime_rate": 48.00,
        "target_strain": 3.5,
        "strain_tol": 1.0,
        "base_orders": 165,
        "order_sd": 28,
        "cycle_time": 0.56,
        "cycle_sd": 0.05,
        "base_abs": 0.050,
    },
    {
        "role_id": "WELDING_SPEC",
        "department": "Machining",
        "base_roster": 16,
        "hourly_wage": 26.00,
        "overtime_rate": 39.00,
        "target_strain": 5.0,
        "strain_tol": 1.0,
        "base_orders": 190,
        "order_sd": 36,
        "cycle_time": 0.60,
        "cycle_sd": 0.07,
        "base_abs": 0.080,
    },
    {
        "role_id": "PCB_ASSEMBLER",
        "department": "Assembly",
        "base_roster": 22,
        "hourly_wage": 22.00,
        "overtime_rate": 33.00,
        "target_strain": 3.8,
        "strain_tol": 1.1,
        "base_orders": 320,
        "order_sd": 48,
        "cycle_time": 0.47,
        "cycle_sd": 0.05,
        "base_abs": 0.060,
    },
    {
        "role_id": "QA_INSPECTOR",
        "department": "Quality_Control",
        "base_roster": 12,
        "hourly_wage": 25.00,
        "overtime_rate": 37.50,
        "target_strain": 3.2,
        "strain_tol": 1.0,
        "base_orders": 260,
        "order_sd": 40,
        "cycle_time": 0.30,
        "cycle_sd": 0.04,
        "base_abs": 0.045,
    },
    {
        "role_id": "FORKLIFT_OP",
        "department": "Logistics",
        "base_roster": 15,
        "hourly_wage": 21.50,
        "overtime_rate": 32.25,
        "target_strain": 4.2,
        "strain_tol": 1.2,
        "base_orders": 290,
        "order_sd": 52,
        "cycle_time": 0.35,
        "cycle_sd": 0.04,
        "base_abs": 0.075,
    },
    {
        "role_id": "PACKAGING_ASSOC",
        "department": "Logistics",
        "base_roster": 20,
        "hourly_wage": 19.00,
        "overtime_rate": 28.50,
        "target_strain": 4.0,
        "strain_tol": 1.3,
        "base_orders": 410,
        "order_sd": 65,
        "cycle_time": 0.32,
        "cycle_sd": 0.03,
        "base_abs": 0.070,
    },
    {
        "role_id": "PLC_MAINT_ENG",
        "department": "Maintenance",
        "base_roster": 10,
        "hourly_wage": 35.00,
        "overtime_rate": 52.50,
        "target_strain": 3.6,
        "strain_tol": 1.0,
        "base_orders": 85,
        "order_sd": 16,
        "cycle_time": 0.78,
        "cycle_sd": 0.08,
        "base_abs": 0.040,
    },
]

PLANT_MULTIPLIERS = {
    "PLANT_01": 1.15,
    "PLANT_02": 1.00,
    "PLANT_03": 0.90,
    "PLANT_04": 1.08,
    "PLANT_05": 0.95,
}


def generate_shift_telemetry(filepath: str, num_records: int = 6200) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    start_date = datetime(2026, 8, 1)
    headers = [
        "shift_id",
        "plant_id",
        "role_id",
        "department",
        "scheduled_headcount",
        "present_headcount",
        "shift_duration_hrs",
        "non_productive_hrs",
        "hourly_wage_usd",
        "overtime_rate_usd",
        "shift_date",
        "assigned_workload_hrs",
        "ergonomic_strain_index",
        "target_strain_index",
        "strain_tolerance",
    ]
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(1, num_records + 1):
            shift_id = f"SHF-{10000 + i}"
            plant_id = random.choice(PLANTS)
            role = random.choice(ROLES)
            p_mult = PLANT_MULTIPLIERS[plant_id]

            scheduled = max(4, int(round(role["base_roster"] * p_mult + random.choice([-1, 0, 0, 1]))))
            abs_rate = min(0.30, max(0.0, random.gauss(role["base_abs"], 0.03)))
            absent = int(round(scheduled * abs_rate))
            present = max(2, scheduled - absent)

            shift_duration = 8.0
            non_prod = round(random.uniform(0.75, 1.35), 2)
            net_nominal_cap = present * (shift_duration - non_prod)

            # Create realistic workload distribution across utilization regimes
            regime = random.random()
            if regime < 0.18:
                # High demand spike / bottleneck shift
                workload_factor = random.uniform(1.12, 1.34)
                strain_excursion = random.uniform(1.2, 3.2)
            elif regime < 0.42:
                # Moderate overtime shift
                workload_factor = random.uniform(0.98, 1.12)
                strain_excursion = random.uniform(0.4, 1.6)
            elif regime < 0.82:
                # Balanced shift
                workload_factor = random.uniform(0.82, 0.97)
                strain_excursion = random.uniform(-0.4, 0.8)
            else:
                # Surplus / under-utilized shift
                workload_factor = random.uniform(0.58, 0.78)
                strain_excursion = random.uniform(-0.8, 0.2)

            assigned_workload = round(net_nominal_cap * workload_factor, 2)
            ergonomic_strain = round(
                max(1.5, role["target_strain"] + strain_excursion), 2
            )
            shift_date = (start_date + timedelta(days=random.randint(0, 89))).strftime("%Y-%m-%d")

            writer.writerow([
                shift_id,
                plant_id,
                role["role_id"],
                role["department"],
                scheduled,
                present,
                shift_duration,
                non_prod,
                f"{role['hourly_wage']:.2f}",
                f"{role['overtime_rate']:.2f}",
                shift_date,
                f"{assigned_workload:.2f}",
                f"{ergonomic_strain:.2f}",
                f"{role['target_strain']:.2f}",
                f"{role['strain_tol']:.2f}",
            ])


def generate_daily_workload_history(filepath: str, days: int = 90) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    start_date = datetime(2026, 8, 1)
    headers = [
        "plant_id",
        "role_id",
        "date",
        "daily_order_units",
        "unit_cycle_time_hrs",
        "daily_required_manhours",
        "absenteeism_rate",
    ]
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for d in range(days):
            curr_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            # Weekly cyclic pattern (higher mid-week production load)
            weekday_boost = 1.08 if (d % 7) in (1, 2, 3) else (0.92 if (d % 7) in (5, 6) else 1.0)
            for plant_id in PLANTS:
                p_mult = PLANT_MULTIPLIERS[plant_id]
                for role in ROLES:
                    mean_orders = role["base_orders"] * p_mult * weekday_boost
                    sd_orders = role["order_sd"] * p_mult
                    daily_orders = max(15, int(round(random.gauss(mean_orders, sd_orders))))
                    cycle_time = max(0.15, round(random.gauss(role["cycle_time"], role["cycle_sd"]), 4))
                    req_manhours = round(daily_orders * cycle_time, 2)
                    abs_rate = round(min(0.25, max(0.01, random.gauss(role["base_abs"], 0.02))), 4)
                    writer.writerow([
                        plant_id,
                        role["role_id"],
                        curr_date,
                        daily_orders,
                        f"{cycle_time:.4f}",
                        f"{req_manhours:.2f}",
                        f"{abs_rate:.4f}",
                    ])


if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    shift_path = os.path.join(base, "data", "raw_shift_telemetry.csv")
    hist_path = os.path.join(base, "data", "daily_workload_history.csv")
    generate_shift_telemetry(shift_path, num_records=6200)
    generate_daily_workload_history(hist_path, days=90)
    print(f"[SUCCESS] Generated {shift_path} (6,200 records) and {hist_path} (3,600 records).")
