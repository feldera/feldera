import subprocess
import plotnine as p9
import pandas as pd
import humanize as hm

from io import StringIO

BACKEND = "Monoio"
PATH = "/tmp/feldera-storage-bench"
MEASURE = True

ONE_MIB = 1024 * 1024
ONE_GIB = 1024 * 1024 * 1024

if True:
    # Big config
    TOTAL_SIZE = str(256 * ONE_GIB)
    THREAD_SHIFT_RANGE = range(0, 6)
    BUFFER_SHIFT_RANGE = range(12, 22)
else:
    # Small config
    TOTAL_SIZE = str(32 * ONE_MIB)
    THREAD_SHIFT_RANGE = range(0, 2)
    BUFFER_SHIFT_RANGE = range(12, 14)


def plot(results):
    results["per_thread_file_size"] = results["per_thread_file_size"].map(
        lambda x: int(x)
    )
    results["threads"] = results["threads"].map(lambda x: int(x))
    results["read_time"] = results["read_time"].map(lambda x: float(x))
    results["write_time"] = results["write_time"].map(lambda x: float(x))

    results["buffer_size_str"] = results["buffer_size"].map(
        lambda x: hm.naturalsize(x, binary=True)
    )
    results["buffer_size_str"] = pd.Categorical(
        results["buffer_size_str"],
        categories=[hm.naturalsize(1 << x, binary=True) for x in BUFFER_SHIFT_RANGE],
    )

    results["Read"] = (
        (results["per_thread_file_size"] * results["threads"]) / ONE_MIB
    ) / results["read_time"]
    results["Write"] = (
        (results["per_thread_file_size"] * results["threads"]) / ONE_MIB
    ) / results["write_time"]

    # Melt the DataFrame to long format
    df_long = pd.melt(
        results,
        id_vars=["buffer_size", "threads", "buffer_size_str"],
        value_vars=["Read", "Write"],
        var_name="operation",
        value_name="tput",
    )

    print(df_long)

    plot = (
        p9.ggplot(
            data=df_long,
            mapping=p9.aes(
                x="threads",
                y="tput",
                group="buffer_size_str",
                color="buffer_size_str",
            ),
        )
        + p9.labs(y="Throughput [MiB/s]")
        + p9.scale_x_continuous(
            breaks=[1 << x for x in THREAD_SHIFT_RANGE], name="# Threads"
        )
        + p9.theme_538()
        + p9.theme(
            legend_position="top",
            legend_title=p9.element_blank(),
            subplots_adjust={"wspace": 0.25},
        )
        + p9.scale_color_brewer(type="qual", palette="Set2")
        + p9.geom_point()
        + p9.geom_line()
        + p9.facet_wrap("~operation", scales="free_y")
    )
    plot.save("disk_throughput_plot.png", width=12, height=5, dpi=300, verbose=False)


if __name__ == "__main__":
    if MEASURE:
        results = pd.DataFrame()
        for thread_shift in THREAD_SHIFT_RANGE:
            for buf_shift in BUFFER_SHIFT_RANGE:
                thread_cnt = 1 << thread_shift
                buf_size = 1 << buf_shift
                per_thread_size = int(int(TOTAL_SIZE) / thread_cnt)

                print(
                    f"thread_cnt={thread_cnt} buf_size={buf_size} per_thread_size={per_thread_size} backend={BACKEND} path={PATH}"
                )
                cmd = [
                    "cargo",
                    "run",
                    "--release",
                    "--bin",
                    "bench",
                    "--",
                    "--backend",
                    BACKEND,
                    "--per-thread-file-size",
                    str(per_thread_size),
                    "--threads",
                    str(thread_cnt),
                    "--buffer-size",
                    str(buf_size),
                    "--path",
                    PATH,
                    "--csv",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)

                if result.returncode != 0:
                    print("Error in subprocess:", result.stderr)
                else:
                    # Parse CSV data into pandas DataFrame
                    csv_data = pd.read_csv(StringIO(result.stdout))
                    results = pd.concat([results, csv_data], ignore_index=True)

        results.to_csv("results.csv", index=False)
    else:
        results = pd.read_csv("results.csv")

    plot(results)
