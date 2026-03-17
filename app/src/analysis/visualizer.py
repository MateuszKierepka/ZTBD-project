import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

matplotlib.use("Agg")

DB_COLORS = {
    "postgres": "#336791",
    "mysql": "#4479A1",
    "mongo": "#47A248",
    "neo4j": "#008CC1",
}

DB_LABELS = {
    "postgres": "PostgreSQL",
    "mysql": "MySQL",
    "mongo": "MongoDB",
    "neo4j": "Neo4j",
}

CATEGORY_ORDER = ["INSERT", "SELECT", "UPDATE", "DELETE"]


class Visualizer:

    def __init__(self, results_dir: Path, output_dir: Path):
        self.results_dir = results_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.df: pd.DataFrame | None = None

    def load_results(self) -> None:
        csvs = sorted(self.results_dir.glob("benchmark_*.csv"))
        if not csvs:
            raise FileNotFoundError(f"No benchmark CSV files in {self.results_dir}")
        frames = []
        for f in csvs:
            frame = pd.read_csv(f)
            frames.append(frame)
            print(f"  Loaded {f.name} ({len(frame)} rows)")
        self.df = pd.concat(frames, ignore_index=True)
        self.df["db_label"] = self.df["database"].map(DB_LABELS)
        self.df["index_label"] = self.df["with_indexes"].map(
            {True: "Z indeksami", False: "Bez indeksów"}
        )
        print(f"  Total: {len(self.df)} rows")

    def generate_all(self) -> list[Path]:
        files = []
        volumes = sorted(self.df["volume"].unique())

        for volume in volumes:
            vdf = self.df[self.df["volume"] == volume]
            has_both = vdf["with_indexes"].nunique() == 2

            files.append(self._chart_crud_by_database(vdf, volume, False))
            if has_both:
                files.append(self._chart_crud_by_database(vdf, volume, True))
                files.append(self._chart_index_comparison(vdf, volume))
                files.append(self._chart_index_heatmap(vdf, volume))

            for cat in CATEGORY_ORDER:
                cdf = vdf[vdf["category"] == cat]
                if not cdf.empty:
                    files.append(self._chart_scenarios_detail(cdf, volume, cat))

        all_volumes = sorted(self.df["volume"].unique())
        if len(all_volumes) > 1:
            files.append(self._chart_volume_scaling(self.df, all_volumes))

        files.append(self._chart_category_summary(self.df))

        explain_files = sorted(self.results_dir.glob("explain/explain_*.json"))
        if explain_files:
            files.append(self._chart_explain_summary(explain_files))

        print(f"\n  Generated {len(files)} charts in {self.output_dir}")
        return files

    def _avg(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["scenario_id", "scenario_name", "category", "database",
                         "db_label", "volume", "with_indexes", "index_label"])
            ["time_ms"]
            .mean()
            .reset_index()
        )

    def _chart_crud_by_database(self, vdf: pd.DataFrame, volume: str,
                                 with_indexes: bool) -> Path:
        label = "with_indexes" if with_indexes else "no_indexes"
        label_pl = "z indeksami" if with_indexes else "bez indeksów"
        avg = self._avg(vdf[vdf["with_indexes"] == with_indexes])

        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle(f"Benchmark CRUD — wolumen {volume} ({label_pl})",
                     fontsize=16, fontweight="bold")

        for ax, cat in zip(axes.flatten(), CATEGORY_ORDER):
            cat_data = avg[avg["category"] == cat].sort_values("scenario_id")
            scenarios = cat_data["scenario_id"].unique()
            databases = ["postgres", "mysql", "mongo", "neo4j"]
            x = np.arange(len(scenarios))
            width = 0.18

            for i, db in enumerate(databases):
                db_data = cat_data[cat_data["database"] == db]
                values = [
                    db_data[db_data["scenario_id"] == s]["time_ms"].values[0]
                    if s in db_data["scenario_id"].values else 0
                    for s in scenarios
                ]
                bars = ax.bar(x + i * width, values, width,
                             label=DB_LABELS[db], color=DB_COLORS[db])
                for bar, val in zip(bars, values):
                    if val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                                f"{val:.1f}", ha="center", va="bottom", fontsize=7)

            ax.set_title(cat, fontsize=13, fontweight="bold")
            ax.set_xlabel("Scenariusz")
            ax.set_ylabel("Czas [ms]")
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(scenarios)
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / f"crud_{volume}_{label}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_scenarios_detail(self, cdf: pd.DataFrame, volume: str,
                                 category: str) -> Path:
        avg = self._avg(cdf)
        scenarios = sorted(avg["scenario_id"].unique())
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        index_variants = sorted(avg["with_indexes"].unique())

        n_scenarios = len(scenarios)
        fig, axes = plt.subplots(1, n_scenarios, figsize=(5 * n_scenarios, 5))
        if n_scenarios == 1:
            axes = [axes]

        fig.suptitle(
            f"{category} — wolumen {volume} (szczegóły per scenariusz)",
            fontsize=14, fontweight="bold",
        )

        for ax, sid in zip(axes, scenarios):
            sdata = avg[avg["scenario_id"] == sid]
            sname = sdata["scenario_name"].iloc[0]
            x = np.arange(len(databases))
            width = 0.35 if len(index_variants) == 2 else 0.6

            for j, wi in enumerate(index_variants):
                wi_data = sdata[sdata["with_indexes"] == wi]
                label = "Z indeksami" if wi else "Bez indeksów"
                values = [
                    wi_data[wi_data["database"] == db]["time_ms"].values[0]
                    if db in wi_data["database"].values else 0
                    for db in databases
                ]
                offset = (j - 0.5) * width if len(index_variants) == 2 else 0
                bars = ax.bar(x + offset + width / 2, values, width,
                             label=label, alpha=0.85)
                for bar, val in zip(bars, values):
                    if val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height(), f"{val:.1f}",
                                ha="center", va="bottom", fontsize=7)

            ax.set_title(f"{sid}: {sname}", fontsize=9)
            ax.set_xticks(x)
            ax.set_xticklabels([DB_LABELS[db] for db in databases],
                               rotation=45, ha="right", fontsize=8)
            ax.set_ylabel("Czas [ms]")
            ax.legend(fontsize=7)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / f"detail_{category.lower()}_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_index_comparison(self, vdf: pd.DataFrame, volume: str) -> Path:
        avg = self._avg(vdf)
        scenarios = sorted(avg["scenario_id"].unique())
        databases = ["postgres", "mysql", "mongo", "neo4j"]

        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle(f"Wpływ indeksów na wydajność — wolumen {volume}",
                     fontsize=16, fontweight="bold")

        for ax, cat in zip(axes.flatten(), CATEGORY_ORDER):
            cat_scenarios = [s for s in scenarios
                            if avg[(avg["scenario_id"] == s)
                                   & (avg["category"] == cat)].shape[0] > 0]
            x = np.arange(len(cat_scenarios))
            width = 0.09

            for i, db in enumerate(databases):
                for j, wi in enumerate([False, True]):
                    data = avg[(avg["category"] == cat) & (avg["database"] == db)
                               & (avg["with_indexes"] == wi)]
                    values = [
                        data[data["scenario_id"] == s]["time_ms"].values[0]
                        if s in data["scenario_id"].values else 0
                        for s in cat_scenarios
                    ]
                    offset = i * width * 2 + j * width
                    pattern = "" if not wi else "///"
                    label = f"{DB_LABELS[db]} {'z idx' if wi else 'bez idx'}"
                    ax.bar(x + offset, values, width, label=label,
                          color=DB_COLORS[db], alpha=0.6 if not wi else 1.0,
                          hatch=pattern)

            ax.set_title(cat, fontsize=13, fontweight="bold")
            ax.set_xlabel("Scenariusz")
            ax.set_ylabel("Czas [ms]")
            ax.set_xticks(x + width * 3.5)
            ax.set_xticklabels(cat_scenarios)
            ax.legend(fontsize=6, ncol=2)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / f"index_comparison_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_index_heatmap(self, vdf: pd.DataFrame, volume: str) -> Path:
        avg = self._avg(vdf)
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        scenarios = sorted(avg["scenario_id"].unique())

        ratio_matrix = np.zeros((len(scenarios), len(databases)))

        for i, sid in enumerate(scenarios):
            for j, db in enumerate(databases):
                no_idx = avg[(avg["scenario_id"] == sid) & (avg["database"] == db)
                             & (avg["with_indexes"] == False)]["time_ms"].values
                with_idx = avg[(avg["scenario_id"] == sid) & (avg["database"] == db)
                               & (avg["with_indexes"] == True)]["time_ms"].values
                if len(no_idx) > 0 and len(with_idx) > 0 and no_idx[0] > 0:
                    ratio_matrix[i, j] = with_idx[0] / no_idx[0]
                else:
                    ratio_matrix[i, j] = 1.0

        fig, ax = plt.subplots(figsize=(10, 12))
        im = ax.imshow(ratio_matrix, cmap="RdYlGn_r", aspect="auto",
                       vmin=0.2, vmax=2.0)

        ax.set_xticks(range(len(databases)))
        ax.set_xticklabels([DB_LABELS[db] for db in databases], fontsize=11)
        ax.set_yticks(range(len(scenarios)))
        ax.set_yticklabels(scenarios, fontsize=10)

        for i in range(len(scenarios)):
            for j in range(len(databases)):
                val = ratio_matrix[i, j]
                color = "white" if val > 1.5 or val < 0.5 else "black"
                ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                       fontsize=9, color=color, fontweight="bold")

        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label("Ratio (z indeksami / bez indeksów)\n<1.0 = indeks pomógł, >1.0 = indeks spowolnił",
                       fontsize=10)

        ax.set_title(f"Heatmapa wpływu indeksów — wolumen {volume}",
                    fontsize=14, fontweight="bold")

        plt.tight_layout()
        path = self.output_dir / f"index_heatmap_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_volume_scaling(self, df: pd.DataFrame,
                               volumes: list[str]) -> Path:
        avg = self._avg(df)
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        volume_order = {"small": 0, "medium": 1, "large": 2}
        volumes = sorted(volumes, key=lambda v: volume_order.get(v, 99))

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("Skalowalność — wpływ wolumenu danych na wydajność",
                     fontsize=16, fontweight="bold")

        for ax, cat in zip(axes.flatten(), CATEGORY_ORDER):
            cat_data = avg[(avg["category"] == cat) & (avg["with_indexes"] == False)]
            scenarios = sorted(cat_data["scenario_id"].unique())

            for db in databases:
                db_data = cat_data[cat_data["database"] == db]
                cat_avgs = []
                for v in volumes:
                    v_data = db_data[db_data["volume"] == v]["time_ms"]
                    cat_avgs.append(v_data.mean() if not v_data.empty else 0)

                ax.plot(volumes, cat_avgs, marker="o", label=DB_LABELS[db],
                       color=DB_COLORS[db], linewidth=2, markersize=8)

            ax.set_title(cat, fontsize=13, fontweight="bold")
            ax.set_xlabel("Wolumen")
            ax.set_ylabel("Średni czas [ms]")
            ax.legend(fontsize=9)
            ax.grid(alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / "volume_scaling.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_category_summary(self, df: pd.DataFrame) -> Path:
        avg = self._avg(df)
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        volumes = sorted(avg["volume"].unique())

        fig, axes = plt.subplots(1, len(volumes), figsize=(7 * len(volumes), 6))
        if len(volumes) == 1:
            axes = [axes]

        fig.suptitle("Podsumowanie — średni czas per kategoria CRUD",
                     fontsize=16, fontweight="bold")

        for ax, volume in zip(axes, volumes):
            vdata = avg[(avg["volume"] == volume) & (avg["with_indexes"] == False)]
            x = np.arange(len(CATEGORY_ORDER))
            width = 0.18

            for i, db in enumerate(databases):
                db_data = vdata[vdata["database"] == db]
                values = [
                    db_data[db_data["category"] == cat]["time_ms"].mean()
                    if cat in db_data["category"].values else 0
                    for cat in CATEGORY_ORDER
                ]
                ax.bar(x + i * width, values, width,
                      label=DB_LABELS[db], color=DB_COLORS[db])

            ax.set_title(f"Wolumen: {volume}", fontsize=12)
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(CATEGORY_ORDER)
            ax.set_ylabel("Średni czas [ms]")
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / "category_summary.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_explain_summary(self, explain_files: list[Path]) -> Path:
        all_data = []
        for f in explain_files:
            with open(f, encoding="utf-8") as fh:
                plans = json.load(fh)
            parts = f.stem.split("_")
            volume = parts[1]
            label = "_".join(parts[2:])

            for sid, sdata in plans.items():
                for db_name, summary in sdata.get("summary", {}).items():
                    if "error" in summary:
                        continue
                    time_val = None
                    if db_name == "postgres":
                        time_val = summary.get("execution_time_ms", 0)
                    elif db_name == "mongo":
                        time_val = summary.get("execution_time_ms", 0)
                    elif db_name == "neo4j":
                        time_val = summary.get("total_db_hits", 0)

                    scan_info = ""
                    if db_name == "postgres":
                        scans = summary.get("scan_types", [])
                        scan_info = ", ".join(s["type"] for s in scans)
                    elif db_name == "mongo":
                        scan_info = summary.get("scan_type", "")
                    elif db_name == "mysql":
                        tables = summary.get("tables", [])
                        scan_info = ", ".join(t.get("access_type", "") for t in tables)

                    all_data.append({
                        "scenario_id": sid,
                        "database": db_name,
                        "volume": volume,
                        "index_label": label,
                        "time_or_hits": time_val,
                        "scan_info": scan_info,
                    })

        if not all_data:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.text(0.5, 0.5, "Brak danych EXPLAIN", ha="center", va="center",
                   fontsize=14)
            path = self.output_dir / "explain_summary.png"
            fig.savefig(path, dpi=150)
            plt.close(fig)
            return path

        edf = pd.DataFrame(all_data)

        pg_data = edf[edf["database"] == "postgres"]
        if pg_data.empty:
            pg_data = edf

        scenarios = sorted(pg_data["scenario_id"].unique())
        volumes = sorted(pg_data["volume"].unique())
        index_labels = sorted(pg_data["index_label"].unique())

        fig, axes = plt.subplots(1, len(index_labels),
                                figsize=(8 * len(index_labels), 6))
        if len(index_labels) == 1:
            axes = [axes]

        fig.suptitle("EXPLAIN — czas wykonania zapytań SELECT (PostgreSQL)",
                     fontsize=14, fontweight="bold")

        for ax, ilabel in zip(axes, index_labels):
            il_data = pg_data[pg_data["index_label"] == ilabel]
            x = np.arange(len(scenarios))
            width = 0.25
            vol_colors = {"small": "#5DA5DA", "medium": "#FAA43A", "large": "#F17CB0"}

            for i, vol in enumerate(volumes):
                vdata = il_data[il_data["volume"] == vol]
                values = [
                    vdata[vdata["scenario_id"] == s]["time_or_hits"].values[0]
                    if s in vdata["scenario_id"].values
                    and vdata[vdata["scenario_id"] == s]["time_or_hits"].values[0] is not None
                    else 0
                    for s in scenarios
                ]
                bars = ax.bar(x + i * width, values, width, label=f"{vol}",
                             color=vol_colors.get(vol, "#999999"))
                for bar, val in zip(bars, values):
                    if val and val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height(), f"{val:.2f}",
                                ha="center", va="bottom", fontsize=7)

            title_label = "z indeksami" if "with" in ilabel else "bez indeksów"
            ax.set_title(f"{title_label}", fontsize=12)
            ax.set_xticks(x + width * (len(volumes) - 1) / 2)
            ax.set_xticklabels(scenarios)
            ax.set_ylabel("Czas wykonania [ms]")
            ax.legend(fontsize=9)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / "explain_summary.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path
