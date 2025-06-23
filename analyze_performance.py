#!/usr/bin/env python3
"""
Enhanced Triangular Arbitrage Performance Analysis Script - Fixed for Corrected Timing

This script analyzes both performance metrics and arbitrage opportunities
from the enhanced triangular arbitrage system with corrected timing measurements.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime


def load_and_clean_data(performance_csv, opportunities_csv):
    """Load both CSV files and perform initial data cleaning."""
    if not os.path.exists(performance_csv):
        print(f"Error: Performance file {performance_csv} not found.")
        sys.exit(1)

    print(f"Loading performance data from {performance_csv}")

    # Read the performance CSV file
    perf_df = pd.read_csv(performance_csv)

    # Convert timestamp to datetime
    perf_df["timestamp"] = pd.to_datetime(perf_df["timestamp"])

    # Calculate time since start for each measurement
    perf_df["seconds_elapsed"] = (
        perf_df["timestamp"] - perf_df["timestamp"].min()
    ).dt.total_seconds()

    # Create a time bucket column for aggregation (every second)
    perf_df["time_bucket"] = perf_df["seconds_elapsed"].astype(int)

    # Load opportunities data if available
    opp_df = None
    if os.path.exists(opportunities_csv):
        print(f"Loading arbitrage opportunities from {opportunities_csv}")
        opp_df = pd.read_csv(opportunities_csv)
        opp_df["timestamp"] = pd.to_datetime(opp_df["timestamp"])
        opp_df["seconds_elapsed"] = (
            opp_df["timestamp"] - opp_df["timestamp"].min()
        ).dt.total_seconds()
    else:
        print(
            f"Opportunities file {opportunities_csv} not found, skipping opportunity analysis"
        )

    return perf_df, opp_df


def create_output_directory(csv_file):
    """Create output directory for analysis results."""
    base_dir = os.path.dirname(csv_file)
    output_dir = os.path.join(base_dir, "analysis")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def plot_timing_over_time(df, output_dir):
    """Create line plots showing how processing times change during the test."""
    plt.figure(figsize=(14, 8))

    # Group by time bucket and calculate mean times
    time_series = df.groupby("time_bucket").agg(
        {
            "sbe_parse_time_us": "mean",
            "orderbook_update_time_us": "mean",
            "arbitrage_detection_time_us": "mean",
            "total_processing_time_us": "mean",
        }
    )

    # Convert to milliseconds
    time_series = time_series / 1000

    # Create subplots for better visualization
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("Processing Times Over Test Duration (CORRECTED)", fontsize=16)

    # Individual component times
    axes[0, 0].plot(
        time_series.index,
        time_series["sbe_parse_time_us"],
        color="blue",
        linewidth=1,
        alpha=0.7,
    )
    axes[0, 0].set_title("SBE Parse Time (Pure)")
    axes[0, 0].set_ylabel("Time (ms)")
    axes[0, 0].grid(True, alpha=0.3)

    axes[0, 1].plot(
        time_series.index,
        time_series["orderbook_update_time_us"],
        color="green",
        linewidth=1,
        alpha=0.7,
    )
    axes[0, 1].set_title("Orderbook Update Time (Pure)")
    axes[0, 1].set_ylabel("Time (ms)")
    axes[0, 1].grid(True, alpha=0.3)

    axes[1, 0].plot(
        time_series.index,
        time_series["arbitrage_detection_time_us"],
        color="orange",
        linewidth=1,
        alpha=0.7,
    )
    axes[1, 0].set_title("Arbitrage Detection Time (Isolated)")
    axes[1, 0].set_xlabel("Seconds Elapsed")
    axes[1, 0].set_ylabel("Time (ms)")
    axes[1, 0].grid(True, alpha=0.3)

    axes[1, 1].plot(
        time_series.index,
        time_series["total_processing_time_us"],
        color="red",
        linewidth=1,
        alpha=0.7,
    )
    axes[1, 1].set_title("Total Processing Time (Sum of Components)")
    axes[1, 1].set_xlabel("Seconds Elapsed")
    axes[1, 1].set_ylabel("Time (ms)")
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "timing_over_time.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def plot_component_breakdown(df, output_dir):
    """Create detailed component breakdown analysis."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(
        "Processing Component Analysis (CORRECTED TIMING)",
        fontsize=16,
        fontweight="bold",
    )

    # Convert to milliseconds for all timing columns
    timing_cols = [
        "sbe_parse_time_us",
        "orderbook_update_time_us",
        "arbitrage_detection_time_us",
        "total_processing_time_us",
    ]
    timing_data_ms = df[timing_cols] / 1000

    # Box plot comparison
    timing_melted = pd.melt(timing_data_ms, var_name="Component", value_name="Time_ms")
    timing_melted["Component"] = (
        timing_melted["Component"]
        .str.replace("_time_us", "")
        .str.replace("_", " ")
        .str.title()
    )

    sns.boxplot(data=timing_melted, x="Component", y="Time_ms", ax=axes[0, 0])
    axes[0, 0].set_title("Component Time Distribution")
    axes[0, 0].set_ylabel("Time (ms)")
    axes[0, 0].tick_params(axis="x", rotation=45)
    axes[0, 0].grid(True, alpha=0.3)

    # Histogram of total processing time
    axes[0, 1].hist(
        timing_data_ms["total_processing_time_us"],
        bins=50,
        alpha=0.7,
        color="red",
        edgecolor="black",
    )
    total_95th = timing_data_ms["total_processing_time_us"].quantile(0.95)
    total_99th = timing_data_ms["total_processing_time_us"].quantile(0.99)
    axes[0, 1].axvline(
        total_95th, color="orange", linestyle="--", label=f"95th: {total_95th:.3f}ms"
    )
    axes[0, 1].axvline(
        total_99th, color="red", linestyle="--", label=f"99th: {total_99th:.3f}ms"
    )
    axes[0, 1].set_title("Total Processing Time Distribution")
    axes[0, 1].set_xlabel("Time (ms)")
    axes[0, 1].set_ylabel("Frequency")
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)

    # Component contribution pie chart
    component_means = timing_data_ms[
        ["sbe_parse_time_us", "orderbook_update_time_us", "arbitrage_detection_time_us"]
    ].mean()
    component_means.index = ["SBE Parse", "Orderbook Update", "Arbitrage Detection"]

    axes[1, 0].pie(
        component_means.values,
        labels=component_means.index,
        autopct="%1.1f%%",
        startangle=90,
    )
    axes[1, 0].set_title("Average Component Time Contribution")

    # Time correlation analysis
    correlation_matrix = timing_data_ms.corr()
    sns.heatmap(
        correlation_matrix, annot=True, cmap="coolwarm", center=0, ax=axes[1, 1]
    )
    axes[1, 1].set_title("Component Time Correlations")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "component_breakdown.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def plot_timing_validation(df, output_dir):
    """Validate that total time equals sum of components."""
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle("Timing Measurement Validation", fontsize=16, fontweight="bold")

    # Calculate sum of components
    df["calculated_total"] = (
        df["sbe_parse_time_us"]
        + df["orderbook_update_time_us"]
        + df["arbitrage_detection_time_us"]
    )

    df["timing_difference"] = df["total_processing_time_us"] - df["calculated_total"]

    # Scatter plot: reported total vs calculated total
    axes[0].scatter(
        df["calculated_total"] / 1000,
        df["total_processing_time_us"] / 1000,
        alpha=0.5,
        s=1,
    )

    # Perfect correlation line
    max_val = (
        max(df["calculated_total"].max(), df["total_processing_time_us"].max()) / 1000
    )
    axes[0].plot([0, max_val], [0, max_val], "r--", label="Perfect Match")

    axes[0].set_xlabel("Calculated Total (ms)")
    axes[0].set_ylabel("Reported Total (ms)")
    axes[0].set_title("Total Time Validation")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # Histogram of timing differences
    axes[1].hist(
        df["timing_difference"], bins=50, alpha=0.7, color="purple", edgecolor="black"
    )
    axes[1].axvline(0, color="red", linestyle="--", label="Perfect Match")
    axes[1].set_title("Timing Measurement Errors")
    axes[1].set_xlabel("Difference (μs)")
    axes[1].set_ylabel("Frequency")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "timing_validation.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()

    # Print validation statistics
    mean_diff = df["timing_difference"].mean()
    std_diff = df["timing_difference"].std()
    max_diff = df["timing_difference"].abs().max()

    print(f"\nTiming Validation Results:")
    print(f"Mean difference: {mean_diff:.2f} μs")
    print(f"Std deviation: {std_diff:.2f} μs")
    print(f"Max absolute difference: {max_diff:.2f} μs")

    if abs(mean_diff) < 1.0 and max_diff < 10.0:
        print("✅ Timing measurements appear to be correctly isolated")
    else:
        print("⚠️  Timing measurements may still have issues")


def plot_symbol_distribution(df, output_dir):
    """Create enhanced symbol distribution analysis."""
    plt.figure(figsize=(15, 10))

    # Count updates per symbol
    symbol_counts = df["symbol"].value_counts().head(30)

    # Create subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

    # Horizontal bar chart for top symbols
    symbol_counts.head(20).plot(kind="barh", ax=ax1, alpha=0.8, color="steelblue")
    ax1.set_title("Top 20 Most Active Symbols", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Number of Updates")
    ax1.grid(True, alpha=0.3)

    # Pie chart for market share of top symbols
    top_10 = symbol_counts.head(10)
    others = symbol_counts.iloc[10:].sum()
    pie_data = list(top_10.values) + [others]
    pie_labels = list(top_10.index) + ["Others"]

    ax2.pie(pie_data, labels=pie_labels, autopct="%1.1f%%", startangle=90)
    ax2.set_title(
        "Market Share of Updates (Top 10 + Others)", fontsize=14, fontweight="bold"
    )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "symbol_distribution.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def generate_enhanced_summary_stats(perf_df, opp_df, output_dir):
    """Generate comprehensive summary statistics."""
    with open(os.path.join(output_dir, "enhanced_summary.txt"), "w") as f:
        f.write("=== ENHANCED TRIANGULAR ARBITRAGE ANALYSIS (CORRECTED TIMING) ===\n")
        f.write(f"Analysis generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Test duration: {perf_df['seconds_elapsed'].max():.2f} seconds\n")
        f.write(f"Total measurements: {len(perf_df):,}\n")
        f.write(
            f"Average throughput: {len(perf_df) / perf_df['seconds_elapsed'].max():.2f} updates/sec\n\n"
        )

        # Performance Statistics
        f.write("=== CORRECTED PERFORMANCE STATISTICS ===\n")
        timing_cols = [
            "sbe_parse_time_us",
            "orderbook_update_time_us",
            "arbitrage_detection_time_us",
            "total_processing_time_us",
        ]

        for col in timing_cols:
            ms_values = perf_df[col] / 1000
            col_name = col.replace("_time_us", "").replace("_", " ").title()

            f.write(f"\n{col_name} (ISOLATED):\n")
            f.write(f"  Mean: {ms_values.mean():.4f} ms\n")
            f.write(f"  Median: {ms_values.median():.4f} ms\n")
            f.write(f"  95th percentile: {ms_values.quantile(0.95):.4f} ms\n")
            f.write(f"  99th percentile: {ms_values.quantile(0.99):.4f} ms\n")
            f.write(f"  99.9th percentile: {ms_values.quantile(0.999):.4f} ms\n")
            f.write(f"  Max: {ms_values.max():.4f} ms\n")
            f.write(f"  Std Dev: {ms_values.std():.4f} ms\n")

        # Timing validation
        f.write(f"\n=== TIMING MEASUREMENT VALIDATION ===\n")
        calculated_total = (
            perf_df["sbe_parse_time_us"]
            + perf_df["orderbook_update_time_us"]
            + perf_df["arbitrage_detection_time_us"]
        )
        timing_diff = perf_df["total_processing_time_us"] - calculated_total

        f.write(f"Mean timing difference: {timing_diff.mean():.2f} μs\n")
        f.write(f"Max absolute timing difference: {timing_diff.abs().max():.2f} μs\n")

        if abs(timing_diff.mean()) < 1.0:
            f.write("✅ Timing measurements are properly isolated\n")
        else:
            f.write("⚠️  Timing measurements may have contamination\n")

        # Component breakdown
        f.write(f"\n=== COMPONENT PERFORMANCE BREAKDOWN ===\n")
        sbe_mean = perf_df["sbe_parse_time_us"].mean() / 1000
        ob_mean = perf_df["orderbook_update_time_us"].mean() / 1000
        arb_mean = perf_df["arbitrage_detection_time_us"].mean() / 1000
        total_mean = perf_df["total_processing_time_us"].mean() / 1000

        f.write(f"SBE Parse: {sbe_mean:.3f} ms ({sbe_mean/total_mean*100:.1f}%)\n")
        f.write(f"Orderbook Update: {ob_mean:.3f} ms ({ob_mean/total_mean*100:.1f}%)\n")
        f.write(
            f"Arbitrage Detection: {arb_mean:.3f} ms ({arb_mean/total_mean*100:.1f}%)\n"
        )
        f.write(f"Total: {total_mean:.3f} ms\n")

        # Symbol Statistics
        f.write(f"\n=== SYMBOL ACTIVITY ===\n")
        symbol_stats = perf_df["symbol"].value_counts()
        f.write(f"Total unique symbols: {len(symbol_stats)}\n")
        f.write(
            f"Most active symbol: {symbol_stats.index[0]} ({symbol_stats.iloc[0]:,} updates)\n"
        )
        f.write(
            f"Least active symbol: {symbol_stats.index[-1]} ({symbol_stats.iloc[-1]:,} updates)\n"
        )
        f.write(f"Average updates per symbol: {symbol_stats.mean():.1f}\n")

        # Arbitrage Statistics
        if opp_df is not None and not opp_df.empty:
            f.write(f"\n=== ARBITRAGE OPPORTUNITIES ===\n")
            f.write(f"Total opportunities found: {len(opp_df):,}\n")
            f.write(
                f"Opportunities per minute: {len(opp_df) / (perf_df['seconds_elapsed'].max() / 60):.2f}\n"
            )
            f.write(f"Average profit: {opp_df['profit_percentage'].mean():.4f}%\n")
            f.write(f"Median profit: {opp_df['profit_percentage'].median():.4f}%\n")
            f.write(f"Best profit: {opp_df['profit_percentage'].max():.4f}%\n")
            f.write(f"Average profit amount: ${opp_df['profit_amount'].mean():.2f}\n")
            f.write(f"Total potential profit: ${opp_df['profit_amount'].sum():.2f}\n")

        # Performance Recommendations
        f.write(f"\n=== PERFORMANCE RECOMMENDATIONS (BASED ON CORRECTED TIMING) ===\n")

        total_99p = perf_df["total_processing_time_us"].quantile(0.99) / 1000
        if total_99p > 10:
            f.write(
                "⚠️  99th percentile total processing time exceeds 10ms - consider optimization\n"
            )
        elif total_99p > 5:
            f.write(
                "⚠️  99th percentile total processing time exceeds 5ms - monitor closely\n"
            )
        else:
            f.write("✅ Total processing time within acceptable limits\n")

        # Identify bottlenecks using corrected measurements
        timing_means = {
            "SBE Parse": perf_df["sbe_parse_time_us"].mean(),
            "Orderbook Update": perf_df["orderbook_update_time_us"].mean(),
            "Arbitrage Detection": perf_df["arbitrage_detection_time_us"].mean(),
        }

        bottleneck = max(timing_means.items(), key=lambda x: x[1])[0]
        f.write(
            f"🔍 Primary bottleneck: {bottleneck} ({timing_means[bottleneck]/1000:.3f}ms avg)\n"
        )

        avg_throughput = len(perf_df) / perf_df["seconds_elapsed"].max()
        if avg_throughput < 100:
            f.write("📉 Low update throughput - consider network optimization\n")
        elif avg_throughput > 1000:
            f.write("🚀 Excellent update throughput\n")
        else:
            f.write("✅ Good update throughput\n")


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python analyze_performance.py <performance_results.csv> [arbitrage_opportunities.csv]"
        )
        sys.exit(1)

    performance_csv = sys.argv[1]
    opportunities_csv = (
        sys.argv[2]
        if len(sys.argv) > 2
        else performance_csv.replace(
            "performance_results.csv", "arbitrage_opportunities.csv"
        )
    )

    try:
        perf_df, opp_df = load_and_clean_data(performance_csv, opportunities_csv)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    output_dir = create_output_directory(performance_csv)
    print(f"Generating enhanced analysis with corrected timing in: {output_dir}")

    try:
        # Generate corrected timing analysis
        plot_timing_over_time(perf_df, output_dir)
        plot_component_breakdown(perf_df, output_dir)
        plot_timing_validation(perf_df, output_dir)
        plot_symbol_distribution(perf_df, output_dir)

        # Generate comprehensive summary
        generate_enhanced_summary_stats(perf_df, opp_df, output_dir)

    except Exception as e:
        print(f"Error generating analysis: {e}")
        sys.exit(1)

    print(
        "Enhanced analysis with corrected timing complete! Check the 'analysis' folder for detailed results."
    )
    print("\nGenerated files:")
    print("- timing_over_time.png: Corrected component timing analysis")
    print("- component_breakdown.png: Detailed component analysis")
    print("- timing_validation.png: Timing measurement validation")
    print("- symbol_distribution.png: Symbol activity analysis")
    print("- enhanced_summary.txt: Comprehensive statistics with corrected timing")


if __name__ == "__main__":
    main()
