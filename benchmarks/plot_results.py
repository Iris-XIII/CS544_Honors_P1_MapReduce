
import csv
from pathlib import Path
from statistics import median
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parent

with (root / "results.csv").open() as f:
    rows = list(csv.DictReader(f))

baseline = [
    float(row["elapsed_seconds"])
    for row in rows
    if row["mode"] == "baseline"
]

speculative = [
    float(row["elapsed_seconds"])
    for row in rows
    if row["mode"] == "speculative"
]

assert len(baseline) == len(speculative) == 3

b = median(baseline)
s = median(speculative)

speedup = b / s
reduction = (1 - s / b) * 100

fig, ax = plt.subplots(figsize=(8, 5))

bars = ax.bar(
    ["Baseline", "Speculative"],
    [b, s],
    color=["#64748b", "#16a34a"],
    width=0.55,
)

# Show the three measured trial runtimes as dots.
for x, values in enumerate([baseline, speculative]):
    offsets = [-0.09, 0, 0.09]

    for offset, value in zip(offsets, values):
        ax.scatter(
            x + offset,
            value,
            color="black",
            s=45,
            marker="o",
            zorder=5,
        )


for bar, value in zip(bars, [b, s]):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        value + 0.30,
        f"{value:.2f}s",
        ha="center",
        fontsize=12,
        fontweight="bold",
    )

ax.set_ylim(0, max(b, s) * 1.2)
ax.set_ylabel("Median job completion time (seconds)")
ax.set_title("MapReduce Speculative Execution Performance")


ax.set_title(
    "MapReduce Speculative Execution Performance",
    fontsize=16,
    pad=55,
)

fig.text(
    0.5,
    0.90,
    "3 trials per configuration | 4 CPU-limited Workers | "
    "8-second artificial straggler | 4 map / 2 reduce tasks",
    ha="center",
    fontsize=9,
    color="#555555",
)

ax.text(
    0.5, 0.99,
    f"{speedup:.2f}x speedup | {reduction:.1f}% runtime reduction",
    ha="center",
    transform=ax.transAxes,
    fontsize=11,
    fontweight="bold",
)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

fig.tight_layout()

output = root / "speculation_performance.png"
fig.savefig(output, dpi=200, bbox_inches="tight")
plt.close(fig)

print(f"Saved: {output}")
print(f"Baseline median: {b:.3f}s")
print(f"Speculative median: {s:.3f}s")
print(f"Speedup: {speedup:.2f}x")