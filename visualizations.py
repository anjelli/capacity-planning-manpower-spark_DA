#!/usr/bin/env python3
"""
Analytical Visualizations Generator (`visualizations.py`)
Reads Apache Spark job outputs from `output/` and produces 3 publication-grade figures
saved in `presentation/figures/`:
  1. utilization_by_department.png
  2. labor_cost_reallocation.png
  3. headcount_policy_fte_buffer.png
"""
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 11,
    "axes.labelsize": 11.5,
    "axes.titlesize": 13,
    "xtick.labelsize": 10.5,
    "ytick.labelsize": 10.5,
    "legend.fontsize": 10.5,
})


def plot_utilization_by_department(job1_csv: str, out_png: str) -> None:
    df = pd.read_csv(job1_csv)
    pivot = (
        df.groupby(["department", "capacity_status"])["shift_count"]
        .sum()
        .unstack(fill_value=0)
    )
    statuses = [
        "OPTIMAL_BALANCED",
        "OVERTIME_WARNING",
        "UNDERSTAFFED_CRITICAL",
        "SURPLUS_IDLE",
    ]
    for s in statuses:
        if s not in pivot.columns:
            pivot[s] = 0
    pivot = pivot[statuses]

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    colors = ["#0F766E", "#D97706", "#DC2626", "#475569"]
    labels = [
        "Optimal Balanced (80-100%)",
        "Overtime Warning (100-115%)",
        "Understaffed Critical (>115%)",
        "Surplus Idle (<80%)",
    ]

    x = np.arange(len(pivot.index))
    bottom = np.zeros(len(pivot.index))
    for col, color, label in zip(statuses, colors, labels):
        vals = pivot[col].values
        ax.bar(x, vals, bottom=bottom, width=0.56, color=color, label=label, edgecolor="white", linewidth=0.8)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels([d.replace("_", " ") for d in pivot.index], fontweight="bold")
    ax.set_ylabel("Number of Shifts (90-Day Horizon)")
    ax.set_title("Shift Utilization Regime Distribution by Department", fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=2, frameon=True)
    fig.tight_layout()
    fig.savefig(out_png, dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_labor_cost_reallocation(job3_csv: str, out_png: str) -> None:
    df = pd.read_csv(job3_csv).sort_values("gross_inefficiency_cost_usd", ascending=False)

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = np.arange(len(df))
    width = 0.27

    gross = df["gross_inefficiency_cost_usd"].values / 1000.0
    savings = df["reallocation_savings_usd"].values / 1000.0
    net_cost = df["net_unrecovered_cost_usd"].values / 1000.0

    ax.bar(x - width, gross, width, color="#1E3A8A", label="Gross Inefficiency Cost ($k)")
    ax.bar(x, savings, width, color="#0D9488", label="Cross-Training Reallocation Savings ($k)")
    ax.bar(x + width, net_cost, width, color="#D97706", label="Net Residual Variance Cost ($k)")

    ax.set_xticks(x)
    ax.set_xticklabels([d.replace("_", " ") for d in df["department"]], fontweight="bold")
    ax.set_ylabel("Labor Variance Cost ($ Thousands)")
    ax.set_title("Gross Overtime & Idle Inefficiency vs. Cross-Training Recovery", fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=3, frameon=True)
    fig.tight_layout()
    fig.savefig(out_png, dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_headcount_policy(job2_csv: str, out_png: str) -> None:
    df = pd.read_csv(job2_csv)
    p1 = df[df["plant_id"] == "PLANT_01"].sort_values("optimal_roster_fte", ascending=False)

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = np.arange(len(p1))
    base_fte = p1["base_fte_req"].values
    safety_fte = p1["safety_buffer_fte"].values
    flex_delta = p1["peak_flex_cap_fte"].values - p1["optimal_roster_fte"].values

    ax.bar(x, base_fte, width=0.52, color="#1E293B", label="Base Workload Headcount (FTE)")
    ax.bar(x, safety_fte, bottom=base_fte, width=0.52, color="#0284C7", label="Stochastic Safety Buffer (Z=1.65)")
    ax.bar(
        x,
        flex_delta,
        bottom=base_fte + safety_fte,
        width=0.52,
        color="#F59E0B",
        label="Peak Flex Pool Cap (+15%)",
    )

    ax.set_xticks(x)
    ax.set_xticklabels(p1["role_id"], rotation=20, ha="right", fontweight="bold")
    ax.set_ylabel("Full-Time Equivalent (FTE) Operators")
    ax.set_title("Stochastic Manpower Policy (Base + Safety Buffer + Flex Cap) — PLANT_01", fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.22), ncol=3, frameon=True)
    fig.tight_layout()
    fig.savefig(out_png, dpi=240, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    base = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base, "output")
    fig_dir = os.path.join(base, "presentation", "figures")
    os.makedirs(fig_dir, exist_ok=True)

    plot_utilization_by_department(
        os.path.join(out_dir, "job1_utilization_risk.csv"),
        os.path.join(fig_dir, "utilization_by_department.png"),
    )
    plot_labor_cost_reallocation(
        os.path.join(out_dir, "job3_cost_reallocation_summary.csv"),
        os.path.join(fig_dir, "labor_cost_reallocation.png"),
    )
    plot_headcount_policy(
        os.path.join(out_dir, "job2_headcount_optimization.csv"),
        os.path.join(fig_dir, "headcount_policy_fte_buffer.png"),
    )
    print(f"[SUCCESS] Generated 3 analytical charts in {fig_dir}")


if __name__ == "__main__":
    main()
