import pandas as pd
import json
from plotnine import *


def get_data_samples(file_path):
    """
    Reads an NDJSON file and returns a list of JSON objects.

    :param file_path: Path to the NDJSON file
    :return: List of JSON objects
    """
    json_list = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            json_list.append(json.loads(line.strip()))  # Parse each line as JSON

    return json_list


# Function to process the data
def parse_data(data_samples):
    records = []
    for timestamp, sample in enumerate(data_samples):
        for entry in sample:
            key = entry["key"]
            value = entry["value"].get("Gauge") or entry["value"].get(
                "Counter"
            )  # Handle Gauge and Counter values
            labels = {
                label[0]: label[1] for label in entry.get("labels", [])
            }  # Convert labels to dict

            # Extract necessary information
            worker = labels.get("worker", "Total")
            level = labels.get("level", "Total")
            spine = labels.get("id", "Total")

            # Append processed entry
            records.append(
                {
                    "timestamp": timestamp,
                    "key": key,
                    "worker": worker,
                    "level": level,
                    "value": value,
                    "id": spine,
                }
            )

    return pd.DataFrame(records)


def make_plots(data_samples):
    # Convert the data
    df = parse_data(data_samples)

    # Filter for specific metrics
    df_merges = df[df["key"] == "spine.ongoing_merges"]
    df_merges_summary = (
        df_merges.groupby(["timestamp", "level"])["value"]
        .agg(["mean", "max", "min"])
        .reset_index()
        .melt(id_vars=["timestamp", "level"], var_name="stat", value_name="value")
    )

    df_batches = df[df["key"] == "spine.batches_per_level"]
    df_batches_summary = (
        df_batches.groupby(["timestamp", "level"])["value"]
        .agg(["mean", "max", "min"])
        .reset_index()
        .melt(id_vars=["timestamp", "level"], var_name="stat", value_name="value")
    )

    # Get bytes written at every time
    df_disk = df[df["key"] == "disk.total_bytes_written"]
    df_disk = df_disk.sort_values("timestamp")
    df_disk["value"] = df_disk["value"].diff() / (1024 * 1024)
    df_disk = df_disk.dropna()
    df_disk["metric"] = "Writes MiB/s"

    # Aggregate total values per timestamp
    df_merges_total = df_merges.groupby("timestamp")["value"].sum().reset_index()
    df_merges_total["worker"] = "Total"
    df_merges_total["level"] = "Total"
    df_merges_total["metric"] = "Current #Batches being merged"

    df_batches_total = df_batches.groupby("timestamp")["value"].sum().reset_index()
    df_batches_total["worker"] = "Total"
    df_batches_total["level"] = "Total"
    df_batches_total["metric"] = "Current #Batches not being merged"

    # Merge with original to include total lines
    df_merges = pd.concat([df_merges_summary])
    df_batches = pd.concat([df_batches_summary])

    df_totals = pd.concat([df_disk, df_merges_total, df_batches_total])

    # Plot function
    def create_plot(df, title, filename):
        plot = (
            ggplot(df, aes(x="timestamp", y="value", color="stat", group="stat"))
            + geom_line(size=1)
            + facet_wrap("~level", scales="free")
            + labs(title=title, x="Time", y="Value")
            + theme_classic()
            + scale_y_continuous(limits=(0, None))
        )
        plot.save(filename, width=12, height=6, dpi=300)
        print(f"Saved {filename}")

    # Generate plots
    create_plot(
        df_merges,
        "Current #Batches being merged (Min/Avg/Max from all Spines)",
        "ongoing_merges.png",
    )
    create_plot(
        df_batches,
        "Current #Batches not being merged (Min/Avg/Max from all Spine)",
        "batches_per_level.png",
    )

    plot = (
        ggplot(df_totals, aes(x="timestamp", y="value", color="metric"))
        + geom_line(size=1)
        + facet_grid(
            "metric ~ .", scales="free_y"
        )  # Separate plots for MiB/s and Counts
        + labs(title="Pipeline Totals", x="Time", y="Value")
        + theme_classic()
        + scale_y_continuous(limits=(0, None))
    )

    # Save the plot
    plot.save("pipeline_totals.png", width=12, height=8, dpi=300)


if __name__ == "__main__":
    import sys

    samples = get_data_samples(sys.argv[1])
    make_plots(samples)
