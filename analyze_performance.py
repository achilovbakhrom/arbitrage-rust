#!/usr/bin/env python3
"""
Triangular Arbitrage Performance Analysis Script

This script analyzes the performance results from the triangular arbitrage system
and generates visualizations to help interpret the data.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime


def load_and_clean_data(csv_file):
    """Load the CSV file and perform initial data cleaning."""
    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found.")
        sys.exit(1)

    print(f"Loading performance data from {csv_file}")

    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Replace NaN values with 0 for percentage columns
    df["best_profit_percentage"] = df["best_profit_percentage"].fillna(0)
    df["avg_profit_percentage"] = df["avg_profit_percentage"].fillna(0)

    # Calculate time since start for each measurement
    df["seconds_elapsed"] = (df["timestamp"] - df["timestamp"].min()).dt.total_seconds()

    # Create a time bucket column for aggregation (every second)
    df["time_bucket"] = df["seconds_elapsed"].astype(int)

    return df


def create_output_directory(csv_file):
    """Create output directory for analysis results."""
    # Get the directory of the CSV file
    base_dir = os.path.dirname(csv_file)

    # Create 'analysis' subdirectory
    output_dir = os.path.join(base_dir, "analysis")
    os.makedirs(output_dir, exist_ok=True)

    return output_dir


def plot_timing_distribution(df, output_dir):
    """Create box plots showing the distribution of processing times."""
    plt.figure(figsize=(10, 6))

    # Select timing columns and melt for seaborn
    timing_columns = [
        "sbe_receive_time_us",
        "orderbook_update_time_us",
        "arbitrage_detection_time_us",
        "total_processing_time_us",
    ]

    timing_df = df[timing_columns].copy()
    # Convert to milliseconds for better readability
    timing_df = timing_df / 1000

    # Rename columns for the plot
    timing_df.columns = [
        "SBE Receive Time (ms)",
        "Orderbook Update Time (ms)",
        "Arbitrage Detection Time (ms)",
        "Total Processing Time (ms)",
    ]

    # Melt the data for seaborn
    melted_df = pd.melt(timing_df)

    # Create the box plot
    sns.boxplot(x="variable", y="value", data=melted_df)
    plt.title("Distribution of Processing Times")
    plt.ylabel("Time (milliseconds)")
    plt.xlabel("")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join(output_dir, "timing_distribution.png"))
    plt.close()


def plot_timing_histograms(df, output_dir):
    """Create histograms for each timing metric."""
    timing_columns = [
        "sbe_receive_time_us",
        "orderbook_update_time_us",
        "arbitrage_detection_time_us",
        "total_processing_time_us",
    ]

    for col in timing_columns:
        plt.figure(figsize=(8, 5))

        # Convert to milliseconds
        values = df[col] / 1000

        # Skip if all values are 0
        if values.max() == 0:
            plt.close()
            continue

        # Create histogram
        sns.histplot(values, kde=True)

        # Format title based on column name
        title = col.replace("_", " ").replace("us", "").title()
        title = title.replace("Sbe", "SBE")

        plt.title(f"{title} (milliseconds)")
        plt.xlabel("Time (ms)")
        plt.tight_layout()

        # Save the plot
        output_name = col.replace("_time_us", "")
        plt.savefig(os.path.join(output_dir, f"{output_name}_histogram.png"))
        plt.close()


def plot_timing_over_time(df, output_dir):
    """Create line plots showing how processing times change during the test."""
    plt.figure(figsize=(12, 6))

    # Group by time bucket and calculate mean times
    time_series = df.groupby("time_bucket").agg(
        {
            "sbe_receive_time_us": "mean",
            "orderbook_update_time_us": "mean",
            "arbitrage_detection_time_us": "mean",
            "total_processing_time_us": "mean",
        }
    )

    # Convert to milliseconds
    time_series = time_series / 1000

    # Rename columns for the plot
    time_series.columns = [
        "SBE Receive Time",
        "Orderbook Update Time",
        "Arbitrage Detection Time",
        "Total Processing Time",
    ]

    # Plot each timing metric
    for col in time_series.columns:
        plt.plot(
            time_series.index, time_series[col], label=col, marker="o", markersize=2
        )

    plt.title("Processing Times Over Test Duration")
    plt.xlabel("Seconds Elapsed")
    plt.ylabel("Time (milliseconds)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join(output_dir, "timing_over_time.png"))
    plt.close()


def plot_updates_per_second(df, output_dir):
    """Plot the number of updates processed each second."""
    plt.figure(figsize=(12, 5))

    # Count updates per second
    updates_per_second = df.groupby("time_bucket")["updates_processed"].sum()

    plt.bar(updates_per_second.index, updates_per_second.values, alpha=0.7)
    plt.title("Updates Processed Per Second")
    plt.xlabel("Seconds Elapsed")
    plt.ylabel("Number of Updates")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join(output_dir, "updates_per_second.png"))
    plt.close()


def plot_opportunities_per_second(df, output_dir):
    """Plot the number of arbitrage opportunities found each second."""
    plt.figure(figsize=(12, 5))

    # Sum opportunities per second
    opps_per_second = df.groupby("time_bucket")["opportunities_found"].sum()

    # Only create plot if there are opportunities
    if opps_per_second.sum() > 0:
        plt.bar(opps_per_second.index, opps_per_second.values, color="green", alpha=0.7)
        plt.title("Arbitrage Opportunities Found Per Second")
        plt.xlabel("Seconds Elapsed")
        plt.ylabel("Number of Opportunities")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save the plot
        plt.savefig(os.path.join(output_dir, "opportunities_per_second.png"))

    plt.close()


def plot_profit_percentages(df, output_dir):
    """Create a scatter plot of profit percentages over time."""
    # Filter to only rows with profit > 0
    profit_df = df[df["best_profit_percentage"] > 0].copy()

    if not profit_df.empty:
        plt.figure(figsize=(12, 5))

        plt.scatter(
            profit_df["seconds_elapsed"],
            profit_df["best_profit_percentage"],
            alpha=0.7,
            color="green",
        )

        plt.title("Profit Percentages Over Time")
        plt.xlabel("Seconds Elapsed")
        plt.ylabel("Profit Percentage (%)")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save the plot
        plt.savefig(os.path.join(output_dir, "profit_percentages.png"))
        plt.close()


def plot_symbol_distribution(df, output_dir):
    """Create a bar chart showing the distribution of symbols."""
    plt.figure(figsize=(12, 6))

    # Count updates per symbol
    symbol_counts = df["symbol"].value_counts().head(20)  # Top 20 symbols

    # Plot horizontal bar chart
    symbol_counts.plot(kind="barh", alpha=0.7)
    plt.title("Most Frequently Updated Symbols")
    plt.xlabel("Number of Updates")
    plt.ylabel("Symbol")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join(output_dir, "symbol_distribution.png"))
    plt.close()


def generate_summary_stats(df, output_dir):
    """Generate summary statistics file."""
    with open(os.path.join(output_dir, "summary_statistics.txt"), "w") as f:
        # Overall statistics
        f.write("=== TRIANGULAR ARBITRAGE PERFORMANCE ANALYSIS ===\n")
        f.write(f"Analysis generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Test duration: {df['seconds_elapsed'].max():.2f} seconds\n")
        f.write(f"Total measurements: {len(df)}\n")
        f.write(f"Updates per second: {len(df) / df['seconds_elapsed'].max():.2f}\n\n")

        # Timing statistics
        f.write("=== TIMING STATISTICS (milliseconds) ===\n")
        timing_columns = [
            "sbe_receive_time_us",
            "orderbook_update_time_us",
            "arbitrage_detection_time_us",
            "total_processing_time_us",
        ]

        for col in timing_columns:
            ms_values = df[col] / 1000  # Convert to milliseconds

            # Format column name
            col_name = col.replace("_", " ").replace("us", "").title()
            col_name = col_name.replace("Sbe", "SBE")

            f.write(f"{col_name}:\n")
            f.write(f"  Average: {ms_values.mean():.4f} ms\n")
            f.write(f"  Median: {ms_values.median():.4f} ms\n")
            f.write(f"  95th percentile: {ms_values.quantile(0.95):.4f} ms\n")
            f.write(f"  99th percentile: {ms_values.quantile(0.99):.4f} ms\n")
            f.write(f"  Max: {ms_values.max():.4f} ms\n\n")

        # Opportunity statistics
        total_opps = df["opportunities_found"].sum()
        f.write("=== ARBITRAGE STATISTICS ===\n")
        f.write(f"Total opportunities found: {total_opps}\n")

        if total_opps > 0:
            profit_df = df[df["best_profit_percentage"] > 0]
            if not profit_df.empty:
                f.write(
                    f"Average profit percentage: {profit_df['best_profit_percentage'].mean():.4f}%\n"
                )
                f.write(
                    f"Max profit percentage: {profit_df['best_profit_percentage'].max():.4f}%\n"
                )
        else:
            f.write("No arbitrage opportunities found during test\n")

        f.write("\n=== RECOMMENDATIONS ===\n")

        # Make recommendations based on timing
        total_99p = df["total_processing_time_us"].quantile(0.99) / 1000
        if total_99p > 10:
            f.write(
                "⚠️ 99th percentile processing time exceeds 10ms. Consider optimization.\n"
            )

        # Identify bottlenecks
        timing_means = {
            "SBE Receive": df["sbe_receive_time_us"].mean(),
            "Orderbook Update": df["orderbook_update_time_us"].mean(),
            "Arbitrage Detection": df["arbitrage_detection_time_us"].mean(),
        }

        bottleneck = max(timing_means.items(), key=lambda x: x[1])[0]
        f.write(f"🔍 Primary bottleneck appears to be in the {bottleneck} process.\n")

        if df["updates_processed"].sum() < 1000:
            f.write(
                "📉 Low update throughput. Consider increasing symbols or checking network connectivity.\n"
            )


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_performance.py <performance_results.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Load and clean the data
    try:
        df = load_and_clean_data(csv_file)
    except Exception as e:
        print(f"Error loading or processing data: {e}")
        sys.exit(1)

    # Create output directory
    output_dir = create_output_directory(csv_file)
    print(f"Generating analysis in: {output_dir}")

    # Generate the plots
    try:
        plot_timing_distribution(df, output_dir)
        plot_timing_histograms(df, output_dir)
        plot_timing_over_time(df, output_dir)
        plot_updates_per_second(df, output_dir)
        plot_opportunities_per_second(df, output_dir)
        plot_profit_percentages(df, output_dir)
        plot_symbol_distribution(df, output_dir)
        generate_summary_stats(df, output_dir)
    except Exception as e:
        print(f"Error generating plots: {e}")
        sys.exit(1)

    print("Analysis complete! Check the 'analysis' folder for results.")


if __name__ == "__main__":
    main()
