#!/usr/bin/env python3
"""
Enhanced Triangular Arbitrage Performance Analysis Script

This script analyzes both performance metrics and arbitrage opportunities
from the enhanced triangular arbitrage system.
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
            "sbe_receive_time_us": "mean",
            "orderbook_update_time_us": "mean",
            "arbitrage_detection_time_us": "mean",
            "total_processing_time_us": "mean",
        }
    )

    # Convert to milliseconds
    time_series = time_series / 1000

    # Create subplots for better visualization
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("Processing Times Over Test Duration", fontsize=16)

    # Individual component times
    axes[0, 0].plot(
        time_series.index,
        time_series["sbe_receive_time_us"],
        color="blue",
        linewidth=1,
        alpha=0.7,
    )
    axes[0, 0].set_title("SBE Receive Time")
    axes[0, 0].set_ylabel("Time (ms)")
    axes[0, 0].grid(True, alpha=0.3)

    axes[0, 1].plot(
        time_series.index,
        time_series["orderbook_update_time_us"],
        color="green",
        linewidth=1,
        alpha=0.7,
    )
    axes[0, 1].set_title("Orderbook Update Time")
    axes[0, 1].set_ylabel("Time (ms)")
    axes[0, 1].grid(True, alpha=0.3)

    axes[1, 0].plot(
        time_series.index,
        time_series["arbitrage_detection_time_us"],
        color="orange",
        linewidth=1,
        alpha=0.7,
    )
    axes[1, 0].set_title("Arbitrage Detection Time")
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
    axes[1, 1].set_title("Total Processing Time")
    axes[1, 1].set_xlabel("Seconds Elapsed")
    axes[1, 1].set_ylabel("Time (ms)")
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "timing_over_time.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_symbol_distribution(df, output_dir):
    """Create enhanced symbol distribution analysis."""
    plt.figure(figsize=(15, 10))

    # Count updates per symbol
    symbol_counts = df["symbol"].value_counts().head(30)  # Top 30 symbols

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


def plot_enhanced_orderbook_update_analysis(df, output_dir):
    """Enhanced orderbook update time analysis."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(
        "Orderbook Update Performance Analysis", fontsize=16, fontweight="bold"
    )

    update_times_ms = df["orderbook_update_time_us"] / 1000

    # Histogram with percentile lines
    axes[0, 0].hist(
        update_times_ms, bins=50, alpha=0.7, color="green", edgecolor="black"
    )
    axes[0, 0].axvline(
        update_times_ms.quantile(0.95),
        color="red",
        linestyle="--",
        label=f"95th percentile: {update_times_ms.quantile(0.95):.2f}ms",
    )
    axes[0, 0].axvline(
        update_times_ms.quantile(0.99),
        color="darkred",
        linestyle="--",
        label=f"99th percentile: {update_times_ms.quantile(0.99):.2f}ms",
    )
    axes[0, 0].set_title("Distribution of Update Times")
    axes[0, 0].set_xlabel("Time (ms)")
    axes[0, 0].set_ylabel("Frequency")
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # Box plot by symbol (top 15 symbols)
    top_symbols = df["symbol"].value_counts().head(15).index
    symbol_data = [
        df[df["symbol"] == symbol]["orderbook_update_time_us"] / 1000
        for symbol in top_symbols
    ]

    bp = axes[0, 1].boxplot(symbol_data, labels=top_symbols, patch_artist=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("lightgreen")
    axes[0, 1].set_title("Update Times by Symbol (Top 15)")
    axes[0, 1].set_ylabel("Time (ms)")
    axes[0, 1].tick_params(axis="x", rotation=45)
    axes[0, 1].grid(True, alpha=0.3)

    # Time series of rolling average
    df_sorted = df.sort_values("timestamp")
    df_sorted["rolling_avg"] = (
        df_sorted["orderbook_update_time_us"].rolling(window=100).mean() / 1000
    )

    axes[1, 0].plot(
        df_sorted["seconds_elapsed"],
        df_sorted["rolling_avg"],
        color="green",
        linewidth=1,
        alpha=0.8,
    )
    axes[1, 0].set_title("Rolling Average Update Time (100-update window)")
    axes[1, 0].set_xlabel("Seconds Elapsed")
    axes[1, 0].set_ylabel("Time (ms)")
    axes[1, 0].grid(True, alpha=0.3)

    # Performance heatmap by time of test
    df["test_minute"] = (df["seconds_elapsed"] // 60).astype(int)
    heatmap_data = (
        df.groupby(["test_minute", "symbol"])["orderbook_update_time_us"]
        .mean()
        .unstack()
    )

    if not heatmap_data.empty and heatmap_data.shape[1] > 1:
        sns.heatmap(
            heatmap_data.iloc[:, :10],
            ax=axes[1, 1],
            cmap="RdYlBu_r",
            cbar_kws={"label": "Avg Update Time (μs)"},
        )
        axes[1, 1].set_title("Update Time Heatmap by Test Minute")
        axes[1, 1].set_xlabel("Symbol")
        axes[1, 1].set_ylabel("Test Minute")
    else:
        axes[1, 1].text(
            0.5,
            0.5,
            "Insufficient data\nfor heatmap",
            ha="center",
            va="center",
            transform=axes[1, 1].transAxes,
        )
        axes[1, 1].set_title("Update Time Heatmap (Insufficient Data)")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "orderbook_update_analysis.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def plot_updates_per_second(df, output_dir):
    """Enhanced updates per second analysis."""
    fig, axes = plt.subplots(2, 1, figsize=(15, 10))
    fig.suptitle("Update Throughput Analysis", fontsize=16, fontweight="bold")

    # Updates per second over time
    updates_per_second = df.groupby("time_bucket")["updates_processed"].sum()

    axes[0].bar(
        updates_per_second.index,
        updates_per_second.values,
        alpha=0.7,
        color="steelblue",
    )
    axes[0].axhline(
        updates_per_second.mean(),
        color="red",
        linestyle="--",
        label=f"Average: {updates_per_second.mean():.1f} updates/sec",
    )
    axes[0].set_title("Updates Processed Per Second")
    axes[0].set_xlabel("Seconds Elapsed")
    axes[0].set_ylabel("Number of Updates")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # Cumulative updates over time
    cumulative_updates = df.groupby("time_bucket")["updates_processed"].sum().cumsum()

    axes[1].plot(
        cumulative_updates.index,
        cumulative_updates.values,
        color="green",
        linewidth=2,
        alpha=0.8,
    )
    axes[1].set_title("Cumulative Updates Processed")
    axes[1].set_xlabel("Seconds Elapsed")
    axes[1].set_ylabel("Total Updates")
    axes[1].grid(True, alpha=0.3)

    # Add throughput statistics
    total_updates = cumulative_updates.iloc[-1]
    total_time = cumulative_updates.index[-1]
    avg_throughput = total_updates / total_time

    axes[1].text(
        0.7,
        0.3,
        f"Total Updates: {total_updates:,}\nAvg Throughput: {avg_throughput:.1f}/sec",
        transform=axes[1].transAxes,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue"),
    )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "updates_per_second.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_arbitrage_opportunities(opp_df, output_dir):
    """Create comprehensive arbitrage opportunity analysis."""
    if opp_df is None or opp_df.empty:
        print("No arbitrage opportunities data to analyze")
        return

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Arbitrage Opportunities Analysis", fontsize=16, fontweight="bold")

    # Profit distribution
    axes[0, 0].hist(
        opp_df["profit_percentage"], bins=30, alpha=0.7, color="gold", edgecolor="black"
    )
    axes[0, 0].axvline(
        opp_df["profit_percentage"].mean(),
        color="red",
        linestyle="--",
        label=f'Mean: {opp_df["profit_percentage"].mean():.4f}%',
    )
    axes[0, 0].set_title("Distribution of Profit Percentages")
    axes[0, 0].set_xlabel("Profit Percentage (%)")
    axes[0, 0].set_ylabel("Frequency")
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # Opportunities over time
    opp_per_second = opp_df.groupby(opp_df["seconds_elapsed"].astype(int)).size()
    axes[0, 1].plot(
        opp_per_second.index,
        opp_per_second.values,
        color="green",
        marker="o",
        markersize=3,
        alpha=0.7,
    )
    axes[0, 1].set_title("Arbitrage Opportunities Over Time")
    axes[0, 1].set_xlabel("Seconds Elapsed")
    axes[0, 1].set_ylabel("Opportunities per Second")
    axes[0, 1].grid(True, alpha=0.3)

    # Top profitable paths
    path_profits = (
        opp_df.groupby("triangular_path")
        .agg({"profit_percentage": ["count", "mean", "max"]})
        .round(4)
    )
    path_profits.columns = ["Count", "Avg_Profit", "Max_Profit"]
    path_profits = path_profits.sort_values("Max_Profit", ascending=False).head(10)

    y_pos = range(len(path_profits))
    axes[1, 0].barh(y_pos, path_profits["Max_Profit"], alpha=0.7, color="orange")
    axes[1, 0].set_yticks(y_pos)
    axes[1, 0].set_yticklabels(
        [path.replace("->", "\n→") for path in path_profits.index], fontsize=8
    )
    axes[1, 0].set_title("Top 10 Most Profitable Paths (Max Profit)")
    axes[1, 0].set_xlabel("Max Profit Percentage (%)")
    axes[1, 0].grid(True, alpha=0.3)

    # Execution time vs profit scatter
    axes[1, 1].scatter(
        opp_df["execution_time_ns"] / 1000,
        opp_df["profit_percentage"],
        alpha=0.6,
        color="purple",
        s=20,
    )
    axes[1, 1].set_title("Execution Time vs Profit")
    axes[1, 1].set_xlabel("Execution Time (μs)")
    axes[1, 1].set_ylabel("Profit Percentage (%)")
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "arbitrage_opportunities.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def plot_price_correlation_analysis(opp_df, output_dir):
    """Analyze price relationships in arbitrage opportunities."""
    if opp_df is None or opp_df.empty:
        return

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("Price Analysis for Arbitrage Paths", fontsize=16, fontweight="bold")

    # Price distribution for each leg
    axes[0, 0].hist(opp_df["price1"], bins=30, alpha=0.5, label="Price 1", color="red")
    axes[0, 0].hist(
        opp_df["price2"], bins=30, alpha=0.5, label="Price 2", color="green"
    )
    axes[0, 0].hist(opp_df["price3"], bins=30, alpha=0.5, label="Price 3", color="blue")
    axes[0, 0].set_title("Price Distribution Across Path Legs")
    axes[0, 0].set_xlabel("Price")
    axes[0, 0].set_ylabel("Frequency")
    axes[0, 0].legend()
    axes[0, 0].set_yscale("log")
    axes[0, 0].grid(True, alpha=0.3)

    # Price correlation matrix
    price_corr = opp_df[["price1", "price2", "price3", "profit_percentage"]].corr()
    sns.heatmap(price_corr, annot=True, cmap="coolwarm", center=0, ax=axes[0, 1])
    axes[0, 1].set_title("Price Correlation Matrix")

    # Profit vs price ratios
    opp_df["price_ratio_12"] = opp_df["price1"] / opp_df["price2"]
    opp_df["price_ratio_23"] = opp_df["price2"] / opp_df["price3"]

    axes[1, 0].scatter(
        opp_df["price_ratio_12"],
        opp_df["profit_percentage"],
        alpha=0.6,
        color="red",
        s=20,
    )
    axes[1, 0].set_title("Profit vs Price Ratio (Leg1/Leg2)")
    axes[1, 0].set_xlabel("Price Ratio (Price1/Price2)")
    axes[1, 0].set_ylabel("Profit Percentage (%)")
    axes[1, 0].grid(True, alpha=0.3)

    axes[1, 1].scatter(
        opp_df["price_ratio_23"],
        opp_df["profit_percentage"],
        alpha=0.6,
        color="blue",
        s=20,
    )
    axes[1, 1].set_title("Profit vs Price Ratio (Leg2/Leg3)")
    axes[1, 1].set_xlabel("Price Ratio (Price2/Price3)")
    axes[1, 1].set_ylabel("Profit Percentage (%)")
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "price_correlation_analysis.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def plot_latency_analysis(df, output_dir):
    """Detailed latency analysis for different components."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("Latency Analysis Deep Dive", fontsize=16, fontweight="bold")

    # Convert to milliseconds for all timing columns
    timing_cols = [
        "sbe_receive_time_us",
        "orderbook_update_time_us",
        "arbitrage_detection_time_us",
        "total_processing_time_us",
    ]
    timing_data = df[timing_cols] / 1000

    # Percentile analysis
    percentiles = [50, 90, 95, 99, 99.9]
    perc_data = []

    for col in timing_cols:
        perc_values = [timing_data[col].quantile(p / 100) for p in percentiles]
        perc_data.append(perc_values)

    perc_df = pd.DataFrame(
        perc_data,
        columns=[f"P{p}" for p in percentiles],
        index=[
            col.replace("_time_us", "").replace("_", " ").title() for col in timing_cols
        ],
    )

    sns.heatmap(perc_df, annot=True, fmt=".3f", cmap="Reds", ax=axes[0, 0])
    axes[0, 0].set_title("Latency Percentiles (ms)")
    axes[0, 0].set_xlabel("Percentile")

    # Latency over test duration (sliding window)
    window_size = 1000  # 1000 measurements window
    df_sorted = df.sort_values("timestamp")

    rolling_latency = (
        df_sorted["total_processing_time_us"].rolling(window=window_size).quantile(0.95)
        / 1000
    )

    axes[0, 1].plot(
        df_sorted["seconds_elapsed"],
        rolling_latency,
        color="red",
        linewidth=1,
        alpha=0.8,
    )
    axes[0, 1].set_title(
        f"95th Percentile Latency (Rolling {window_size}-sample window)"
    )
    axes[0, 1].set_xlabel("Seconds Elapsed")
    axes[0, 1].set_ylabel("Latency (ms)")
    axes[0, 1].grid(True, alpha=0.3)

    # Component breakdown
    component_means = timing_data.mean()
    component_stds = timing_data.std()

    x_pos = range(len(component_means))
    axes[1, 0].bar(
        x_pos,
        component_means,
        yerr=component_stds,
        alpha=0.7,
        color=["blue", "green", "orange", "red"],
        capsize=5,
    )
    axes[1, 0].set_xticks(x_pos)
    axes[1, 0].set_xticklabels(
        [col.replace("_time_us", "").replace("_", "\n").title() for col in timing_cols],
        rotation=0,
    )
    axes[1, 0].set_title("Average Component Latencies (±1 std dev)")
    axes[1, 0].set_ylabel("Time (ms)")
    axes[1, 0].grid(True, alpha=0.3)

    # Latency distribution comparison (violin plot)
    timing_melted = pd.melt(timing_data, var_name="Component", value_name="Latency_ms")
    timing_melted["Component"] = (
        timing_melted["Component"]
        .str.replace("_time_us", "")
        .str.replace("_", " ")
        .str.title()
    )

    sns.violinplot(data=timing_melted, x="Component", y="Latency_ms", ax=axes[1, 1])
    axes[1, 1].set_title("Latency Distribution by Component")
    axes[1, 1].set_xlabel("")
    axes[1, 1].tick_params(axis="x", rotation=45)
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "latency_analysis.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def generate_enhanced_summary_stats(perf_df, opp_df, output_dir):
    """Generate comprehensive summary statistics."""
    with open(os.path.join(output_dir, "enhanced_summary.txt"), "w") as f:
        f.write("=== ENHANCED TRIANGULAR ARBITRAGE ANALYSIS ===\n")
        f.write(f"Analysis generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Test duration: {perf_df['seconds_elapsed'].max():.2f} seconds\n")
        f.write(f"Total measurements: {len(perf_df):,}\n")
        f.write(
            f"Average throughput: {len(perf_df) / perf_df['seconds_elapsed'].max():.2f} updates/sec\n\n"
        )

        # Performance Statistics
        f.write("=== PERFORMANCE STATISTICS ===\n")
        timing_cols = [
            "sbe_receive_time_us",
            "orderbook_update_time_us",
            "arbitrage_detection_time_us",
            "total_processing_time_us",
        ]

        for col in timing_cols:
            ms_values = perf_df[col] / 1000
            col_name = (
                col.replace("_time_us", "")
                .replace("_", " ")
                .title()
                .replace("Sbe", "SBE")
            )

            f.write(f"\n{col_name}:\n")
            f.write(f"  Mean: {ms_values.mean():.4f} ms\n")
            f.write(f"  Median: {ms_values.median():.4f} ms\n")
            f.write(f"  95th percentile: {ms_values.quantile(0.95):.4f} ms\n")
            f.write(f"  99th percentile: {ms_values.quantile(0.99):.4f} ms\n")
            f.write(f"  99.9th percentile: {ms_values.quantile(0.999):.4f} ms\n")
            f.write(f"  Max: {ms_values.max():.4f} ms\n")
            f.write(f"  Std Dev: {ms_values.std():.4f} ms\n")

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

            # Top paths
            top_paths = (
                opp_df.groupby("triangular_path")
                .agg({"profit_percentage": ["count", "mean", "max"]})
                .round(4)
            )
            top_paths.columns = ["Count", "Avg_Profit", "Max_Profit"]
            top_paths = top_paths.sort_values("Count", ascending=False).head(5)

            f.write(f"\nTop 5 Most Active Arbitrage Paths:\n")
            for path, row in top_paths.iterrows():
                f.write(
                    f"  {path}: {row['Count']} opportunities, "
                    f"avg {row['Avg_Profit']:.4f}%, max {row['Max_Profit']:.4f}%\n"
                )
        else:
            f.write(f"\n=== ARBITRAGE OPPORTUNITIES ===\n")
            f.write("No arbitrage opportunities data available\n")

        # Performance Recommendations
        f.write(f"\n=== PERFORMANCE RECOMMENDATIONS ===\n")

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

        # Identify bottlenecks
        timing_means = {
            "SBE Receive": perf_df["sbe_receive_time_us"].mean(),
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
    print(f"Generating enhanced analysis in: {output_dir}")

    try:
        # Generate core performance plots
        plot_timing_over_time(perf_df, output_dir)
        plot_symbol_distribution(perf_df, output_dir)
        plot_enhanced_orderbook_update_analysis(perf_df, output_dir)
        plot_updates_per_second(perf_df, output_dir)
        plot_latency_analysis(perf_df, output_dir)

        # Generate arbitrage-specific plots if data available
        if opp_df is not None and not opp_df.empty:
            plot_arbitrage_opportunities(opp_df, output_dir)
            plot_price_correlation_analysis(opp_df, output_dir)

        # Generate comprehensive summary
        generate_enhanced_summary_stats(perf_df, opp_df, output_dir)

    except Exception as e:
        print(f"Error generating analysis: {e}")
        sys.exit(1)

    print(
        "Enhanced analysis complete! Check the 'analysis' folder for detailed results."
    )
    print("\nGenerated files:")
    print("- timing_over_time.png: Component timing analysis")
    print("- symbol_distribution.png: Symbol activity analysis")
    print("- orderbook_update_analysis.png: Detailed orderbook performance")
    print("- updates_per_second.png: Throughput analysis")
    print("- latency_analysis.png: Deep latency analysis")
    if opp_df is not None and not opp_df.empty:
        print("- arbitrage_opportunities.png: Opportunity analysis")
        print("- price_correlation_analysis.png: Price relationship analysis")
    print("- enhanced_summary.txt: Comprehensive statistics and recommendations")


if __name__ == "__main__":
    main()
