####################################
# Import Libraries
####################################
# Data manipulation
import pandas as pd

# Plotting
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# System and utilities
from pathlib import Path

####################################
# Define Directories and Paths
####################################
# Project root is one level up from this script, so it works from any working directory
base_dir = Path(__file__).resolve().parent.parent
wrangled_fights_path = base_dir / "data" / "wrangled" / "wrangled_fight_details.csv"
images_dir = base_dir / "images"
images_dir.mkdir(exist_ok=True)

# Outcome groups and their colors, in stacking order
outcome_colors = {
    "Decision": "#2a78d6",
    "KO/TKO": "#eb6834",
    "Submission": "#1baf7a",
    "Other": "#eda100",
}


def outcome_group(method):
    """Groups the method column into decision, KO/TKO, submission, or other (DQ, overturned, etc.)."""
    if not isinstance(method, str):
        return "Other"
    if method.startswith("Decision"):
        return "Decision"
    if method.startswith("KO/TKO") or method.startswith("TKO"):
        return "KO/TKO"
    if method == "Submission":
        return "Submission"
    return "Other"


####################################
# Fights per Year by Outcome
####################################
def plot_fights_per_year():
    """Saves a stacked bar chart of fights per year, split by how the fight ended."""
    df = pd.read_csv(wrangled_fights_path, parse_dates=["date"], low_memory=False)

    # The last scraped year is partial, so leave it out
    last_year = df["date"].dt.year.max()
    df = df[df["date"].dt.year < last_year].copy()

    df["outcome"] = df["method"].apply(outcome_group)
    counts = (
        df.groupby([df["date"].dt.year, "outcome"]).size()
        .unstack(fill_value=0)
        .reindex(columns=list(outcome_colors), fill_value=0)
    )

    fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
    bottom = None
    for outcome, color in outcome_colors.items():
        ax.bar(counts.index, counts[outcome], bottom=bottom, color=color, label=outcome,
               width=0.8, edgecolor="white", linewidth=1)
        bottom = counts[outcome] if bottom is None else bottom + counts[outcome]

    ax.set_title(f"UFC fights per year by outcome, {counts.index.min()} to {counts.index.max()}",
                 loc="left", fontsize=13, color="#0b0b0b")
    ax.set_ylabel("Fights", color="#52514e")
    ax.tick_params(colors="#52514e")
    ax.grid(axis="y", color="#e6e5e1", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ["top", "right", "left"]:
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color("#c3c2b7")
    ax.legend(frameon=False, loc="upper left", ncols=4)

    fig.tight_layout()
    fig.savefig(images_dir / "fights_per_year.png", facecolor="white")
    plt.close(fig)

    print(counts.tail())
    print(f"\nShare of fights by outcome:\n{df['outcome'].value_counts(normalize=True).round(3)}")


if __name__ == "__main__":
    plot_fights_per_year()
