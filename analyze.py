import csv
import matplotlib.pyplot as plt
from pathlib import Path

EXPERIMENT_FOLDER = Path("./experiments/current")
assert EXPERIMENT_FOLDER.is_dir()


def plot_with_endpoint_sorting_and_staggered_callouts(
    xy_series, xlabel, ylabel, title, out_filename, colors
):
    plt.figure(figsize=(10.24, 10.24), dpi=100)

    # 1) Plot all lines and collect endpoints
    endpoints = []
    for idx, (label, points) in enumerate(xy_series.items()):
        pts = sorted(points, key=lambda xy: xy[0])
        xs, ys = zip(*pts)
        color = colors[idx % len(colors)]
        (line,) = plt.plot(xs, ys, marker="o", label=label, color=color)
        endpoints.append((label, line, xs[-1], ys[-1], color))

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    # 2) Sort endpoints by their true y_end ascending
    endpoints.sort(key=lambda tup: tup[3])  # tup[3] is y_end

    # 3) Compute small offsets
    y_values = [tup[3] for tup in endpoints]
    y_min, y_max = min(y_values), max(y_values)
    dy = (y_max - y_min) * 0.03
    x_min, x_max = plt.xlim()
    dx = (x_max - x_min) * 0.02

    # 4) Annotate each endpoint, staggered
    N = len(endpoints)
    for i, (label, line, x_end, y_end, color) in enumerate(endpoints):
        y_text = y_end + (i - N / 2) * dy
        plt.annotate(
            label,
            xy=(x_end, y_end),
            xytext=(x_end + dx, y_text),
            textcoords="data",
            va="center",
            fontsize=9,
            arrowprops=dict(
                arrowstyle="->",
                color=color,
                lw=0.5,
                shrinkA=2,
                shrinkB=2,
            ),
        )

    # 5) Build legend sorted by those same endpoint y’s
    handles = [ep[1] for ep in endpoints]  # ep[1] is the Line2D handle
    labels = [ep[0] for ep in endpoints]  # ep[0] is the label string
    plt.legend(handles, labels, loc="best", frameon=True)

    plt.tight_layout()
    plt.savefig(out_filename)
    plt.close()


def main():
    data = []
    with open(EXPERIMENT_FOLDER / "benchmark_results.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row["RecordCount"] = int(row["RecordCount"])
            row["Duration_us"] = float(row["Duration_us"])
            row["StorageBytes"] = int(row["StorageBytes"])
            row["StorageKB"] = row["StorageBytes"] / 1024.0
            row["Variant"] = f"{row['Strategy']} ({row['Insert']})"
            data.append(row)

    operations = sorted({r["Operation"] for r in data})

    colors = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
        "#393b79",
        "#5254a3",
        "#6b6ecf",
        "#9c9ede",
        "#637939",
        "#8ca252",
        "#b5cf6b",
        "#cedb9c",
        "#8c6d31",
        "#bd9e39",
    ]

    for op in operations:
        series = {}
        for r in data:
            if r["Operation"] != op:
                continue
            series.setdefault(r["Variant"], []).append(
                (r["RecordCount"], r["Duration_us"])
            )
        plot_with_endpoint_sorting_and_staggered_callouts(
            xy_series=series,
            xlabel="Record Count",
            ylabel=f"{op} Time (μs)",
            title=f"{op} Time vs. Record Count",
            out_filename=EXPERIMENT_FOLDER / f"{op}_time.png",
            colors=colors,
        )

    # Storage plot for 'Write'
    storage_series = {}
    for r in data:
        if r["Operation"] != "Write":
            continue
        storage_series.setdefault(r["Variant"], []).append(
            (r["RecordCount"], r["StorageKB"])
        )

    plot_with_endpoint_sorting_and_staggered_callouts(
        xy_series=storage_series,
        xlabel="Record Count",
        ylabel="Storage (KB)",
        title="Storage vs. Record Count",
        out_filename=EXPERIMENT_FOLDER / "storage_growth.png",
        colors=colors,
    )


if __name__ == "__main__":
    main()
