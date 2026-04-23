import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.patches import Patch
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

VOLUME_ORDER = {"small": 0, "medium": 1, "large": 2}


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

            for sid in sorted(vdf["scenario_id"].unique(),
                              key=lambda s: (s[0], int(s[1:]) if s[1:].isdigit() else 0)):
                sdf = vdf[vdf["scenario_id"] == sid]
                if not sdf.empty:
                    files.append(self._chart_per_scenario(sdf, volume, sid))

        all_volumes = sorted(self.df["volume"].unique())
        if len(all_volumes) > 1:
            files.append(self._chart_volume_scaling(self.df, all_volumes))

        files.append(self._chart_category_summary(self.df))

        explain_files = sorted(self.results_dir.glob("explain/explain_*.json"))
        if explain_files:
            explain_data = self._load_explain_data(explain_files)
            if explain_data:
                explain_volumes = sorted(
                    set(d["volume"] for d in explain_data),
                    key=lambda v: VOLUME_ORDER.get(v, 99))
                for vol in explain_volumes:
                    vol_data = [d for d in explain_data if d["volume"] == vol]
                    files.append(self._chart_explain_scan_changes(vol_data, vol))
                    files.append(self._chart_explain_rows_reduction(vol_data, vol))
                    files.append(self._chart_explain_exec_time(vol_data, vol))

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

    def _median(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["scenario_id", "scenario_name", "category", "database",
                         "db_label", "volume", "with_indexes", "index_label"])
            ["time_ms"]
            .median()
            .reset_index()
        )

    def _stats(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["scenario_id", "scenario_name", "category", "database",
                         "db_label", "volume", "with_indexes", "index_label"])
            ["time_ms"]
            .agg(["median", "min", "max"])
            .reset_index()
        )

    @staticmethod
    def _auto_log_scale(ax):
        ylim = ax.get_ylim()
        if ylim[1] > 0 and ylim[1] / max(ylim[0], 0.1) > 100:
            ax.set_yscale("log")

    def _chart_crud_by_database(self, vdf: pd.DataFrame, volume: str,
                                 with_indexes: bool) -> Path:
        label = "with_indexes" if with_indexes else "no_indexes"
        label_pl = "z indeksami" if with_indexes else "bez indeksów"
        stats = self._stats(vdf[vdf["with_indexes"] == with_indexes])

        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle(f"Benchmark CRUD — wolumen {volume} ({label_pl})",
                     fontsize=16, fontweight="bold")

        for ax, cat in zip(axes.flatten(), CATEGORY_ORDER):
            cat_data = stats[stats["category"] == cat].sort_values("scenario_id")
            scenarios = cat_data["scenario_id"].unique()
            databases = ["postgres", "mysql", "mongo", "neo4j"]
            x = np.arange(len(scenarios))
            width = 0.18

            for i, db in enumerate(databases):
                db_data = cat_data[cat_data["database"] == db]
                values, err_lo, err_hi = [], [], []
                for s in scenarios:
                    s_data = db_data[db_data["scenario_id"] == s]
                    if not s_data.empty:
                        med = s_data["median"].values[0]
                        values.append(med)
                        err_lo.append(med - s_data["min"].values[0])
                        err_hi.append(s_data["max"].values[0] - med)
                    else:
                        values.append(0)
                        err_lo.append(0)
                        err_hi.append(0)
                bars = ax.bar(x + i * width, values, width,
                             label=DB_LABELS[db], color=DB_COLORS[db],
                             yerr=[err_lo, err_hi], capsize=2,
                             error_kw={"linewidth": 0.8})
                for bar, val in zip(bars, values):
                    if val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                                f"{val:.1f}", ha="center", va="bottom", fontsize=7)

            ax.set_title(cat, fontsize=13, fontweight="bold")
            ax.set_xlabel("Scenariusz")
            ax.set_ylabel("Mediana czasu [ms]")
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(scenarios)
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=0.3)
            self._auto_log_scale(ax)

        plt.tight_layout()
        path = self.output_dir / f"crud_{volume}_{label}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_scenarios_detail(self, cdf: pd.DataFrame, volume: str,
                                 category: str) -> Path:
        stats = self._stats(cdf)
        scenarios = sorted(stats["scenario_id"].unique())
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        index_variants = sorted(stats["with_indexes"].unique())

        n_scenarios = len(scenarios)
        fig, axes = plt.subplots(1, n_scenarios, figsize=(5 * n_scenarios, 5))
        if n_scenarios == 1:
            axes = [axes]

        fig.suptitle(
            f"{category} — wolumen {volume} (szczegóły per scenariusz)",
            fontsize=14, fontweight="bold",
        )

        for ax, sid in zip(axes, scenarios):
            sdata = stats[stats["scenario_id"] == sid]
            sname = sdata["scenario_name"].iloc[0]
            x = np.arange(len(databases))
            width = 0.35 if len(index_variants) == 2 else 0.6

            for j, wi in enumerate(index_variants):
                wi_data = sdata[sdata["with_indexes"] == wi]
                label = "Z indeksami" if wi else "Bez indeksów"
                values, err_lo, err_hi = [], [], []
                for db in databases:
                    db_row = wi_data[wi_data["database"] == db]
                    if not db_row.empty:
                        med = db_row["median"].values[0]
                        values.append(med)
                        err_lo.append(med - db_row["min"].values[0])
                        err_hi.append(db_row["max"].values[0] - med)
                    else:
                        values.append(0)
                        err_lo.append(0)
                        err_hi.append(0)
                offset = (j - 0.5) * width if len(index_variants) == 2 else 0
                bars = ax.bar(x + offset + width / 2, values, width,
                             label=label, alpha=0.85,
                             yerr=[err_lo, err_hi], capsize=2,
                             error_kw={"linewidth": 0.8})
                for bar, val in zip(bars, values):
                    if val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height(), f"{val:.1f}",
                                ha="center", va="bottom", fontsize=7)

            ax.set_title(f"{sid}: {sname}", fontsize=9)
            ax.set_xticks(x)
            ax.set_xticklabels([DB_LABELS[db] for db in databases],
                               rotation=45, ha="right", fontsize=8)
            ax.set_ylabel("Mediana czasu [ms]")
            ax.legend(fontsize=7)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / f"detail_{category.lower()}_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_per_scenario(self, sdf: pd.DataFrame, volume: str,
                            scenario_id: str) -> Path:
        stats = self._stats(sdf)
        scenario_name = stats["scenario_name"].iloc[0]
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        index_variants = sorted(stats["with_indexes"].unique())

        fig, ax = plt.subplots(figsize=(9, 7))
        x = np.arange(len(databases))
        width = 0.35 if len(index_variants) == 2 else 0.6

        variant_colors = {False: "#1f77b4", True: "#ff7f0e"}
        variant_labels = {False: "Bez indeksów", True: "Z indeksami"}

        for j, wi in enumerate(index_variants):
            wi_data = stats[stats["with_indexes"] == wi]
            values, err_lo, err_hi = [], [], []
            for db in databases:
                db_row = wi_data[wi_data["database"] == db]
                if not db_row.empty:
                    med = db_row["median"].values[0]
                    values.append(med)
                    err_lo.append(med - db_row["min"].values[0])
                    err_hi.append(db_row["max"].values[0] - med)
                else:
                    values.append(0)
                    err_lo.append(0)
                    err_hi.append(0)

            offset = (j - 0.5) * width if len(index_variants) == 2 else 0
            bars = ax.bar(x + offset + width / 2, values, width,
                          label=variant_labels[wi], color=variant_colors[wi],
                          yerr=[err_lo, err_hi], capsize=4,
                          error_kw={"linewidth": 1.0})
            for bar, val in zip(bars, values):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height(), f"{val:.1f}",
                            ha="center", va="bottom", fontsize=9)

        ax.set_title(f"{scenario_id}: {scenario_name}",
                     fontsize=13, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([DB_LABELS[db] for db in databases],
                           rotation=30, ha="right", fontsize=10)
        ax.set_ylabel("Mediana czasu [ms]", fontsize=11)
        ax.legend(fontsize=10, loc="upper left")
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        volume_dir = self.output_dir / volume
        volume_dir.mkdir(parents=True, exist_ok=True)
        path = volume_dir / f"chart_{scenario_id}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_index_comparison(self, vdf: pd.DataFrame, volume: str) -> Path:
        avg = self._median(vdf)
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
            ax.set_ylabel("Mediana czasu [ms]")
            ax.set_xticks(x + width * 3.5)
            ax.set_xticklabels(cat_scenarios)
            ax.legend(fontsize=6, ncol=2)
            ax.grid(axis="y", alpha=0.3)
            self._auto_log_scale(ax)

        plt.tight_layout()
        path = self.output_dir / f"index_comparison_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_index_heatmap(self, vdf: pd.DataFrame, volume: str) -> Path:
        avg = self._median(vdf)
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
        avg = self._median(df)
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        volumes = sorted(volumes, key=lambda v: VOLUME_ORDER.get(v, 99))

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("Skalowalność — wpływ wolumenu danych na wydajność (mediana)",
                     fontsize=16, fontweight="bold")

        for ax, cat in zip(axes.flatten(), CATEGORY_ORDER):
            cat_data = avg[(avg["category"] == cat) & (avg["with_indexes"] == False)]
            scenarios = sorted(cat_data["scenario_id"].unique())

            for db in databases:
                db_data = cat_data[cat_data["database"] == db]
                cat_meds = []
                for v in volumes:
                    v_data = db_data[db_data["volume"] == v]["time_ms"]
                    cat_meds.append(v_data.median() if not v_data.empty else 0)

                ax.plot(volumes, cat_meds, marker="o", label=DB_LABELS[db],
                       color=DB_COLORS[db], linewidth=2, markersize=8)

            ax.set_title(cat, fontsize=13, fontweight="bold")
            ax.set_xlabel("Wolumen")
            ax.set_ylabel("Mediana czasu [ms]")
            ax.legend(fontsize=9)
            ax.grid(alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / "volume_scaling.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_category_summary(self, df: pd.DataFrame) -> Path:
        avg = self._median(df)
        databases = ["postgres", "mysql", "mongo", "neo4j"]
        volumes = sorted(avg["volume"].unique())

        fig, axes = plt.subplots(1, len(volumes), figsize=(7 * len(volumes), 6))
        if len(volumes) == 1:
            axes = [axes]

        fig.suptitle("Podsumowanie — mediana czasu per kategoria CRUD",
                     fontsize=16, fontweight="bold")

        for ax, volume in zip(axes, volumes):
            vdata = avg[(avg["volume"] == volume) & (avg["with_indexes"] == False)]
            x = np.arange(len(CATEGORY_ORDER))
            width = 0.18

            for i, db in enumerate(databases):
                db_data = vdata[vdata["database"] == db]
                values = [
                    db_data[db_data["category"] == cat]["time_ms"].median()
                    if cat in db_data["category"].values else 0
                    for cat in CATEGORY_ORDER
                ]
                ax.bar(x + i * width, values, width,
                      label=DB_LABELS[db], color=DB_COLORS[db])

            ax.set_title(f"Wolumen: {volume}", fontsize=12)
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(CATEGORY_ORDER)
            ax.set_ylabel("Mediana czasu [ms]")
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        path = self.output_dir / "category_summary.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _load_explain_data(self, explain_files: list[Path]) -> list[dict]:
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

                    scan_info = ""
                    rows_examined = None
                    exec_time = None

                    if db_name == "postgres":
                        scans = summary.get("scan_types", [])
                        unique_scans = list(dict.fromkeys(s["type"] for s in scans))
                        scan_info = ", ".join(unique_scans)
                        rows_examined = sum(s.get("rows", 0) for s in scans)
                        exec_time = summary.get("execution_time_ms")
                    elif db_name == "mysql":
                        tables = summary.get("tables", [])
                        unique_access = list(dict.fromkeys(
                            t.get("access_type", "") for t in tables if t.get("access_type")))
                        scan_info = ", ".join(unique_access)
                        rows_examined = sum(t.get("rows_examined", 0) for t in tables)
                    elif db_name == "mongo":
                        scan_info = summary.get("scan_type", "")
                        rows_examined = summary.get("total_docs_examined")
                        exec_time = summary.get("execution_time_ms")
                    elif db_name == "neo4j":
                        ops = summary.get("operators", [])
                        scan_ops = [o for o in ops if "Scan" in o or "Seek" in o]
                        scan_info = ", ".join(scan_ops) if scan_ops else ops[-1] if ops else ""
                        scan_info = scan_info.replace("@neo4j", "")
                        rows_examined = summary.get("total_db_hits")

                    all_data.append({
                        "scenario_id": sid,
                        "scenario_name": sdata.get("name", ""),
                        "database": db_name,
                        "volume": volume,
                        "index_label": label,
                        "scan_info": scan_info,
                        "rows_examined": rows_examined,
                        "exec_time_ms": exec_time,
                    })
        return all_data

    def _chart_explain_scan_changes(self, all_data: list[dict], volume: str) -> Path:
        scenarios = sorted(set(d["scenario_id"] for d in all_data))
        databases = ["postgres", "mysql", "mongo", "neo4j"]

        lookup = {}
        for d in all_data:
            lookup[(d["scenario_id"], d["database"], d["index_label"])] = d

        has_both = any(d["index_label"] == "no_indexes" for d in all_data) and \
                   any(d["index_label"] == "with_indexes" for d in all_data)

        scan_matrix = []
        for sid in scenarios:
            row = []
            for db in databases:
                before = lookup.get((sid, db, "no_indexes"), {})
                after = lookup.get((sid, db, "with_indexes"), {})
                scan_before = before.get("scan_info", "—")
                scan_after = after.get("scan_info", "—")
                if has_both and scan_before and scan_after:
                    row.append((scan_before, scan_after))
                else:
                    row.append((scan_before, ""))
            scan_matrix.append(row)

        color_matrix = np.ones((len(scenarios), len(databases)))
        for i, row in enumerate(scan_matrix):
            for j, (before, after) in enumerate(row):
                if not has_both or not after or before == after:
                    color_matrix[i, j] = 0.5
                elif before != after:
                    color_matrix[i, j] = 0.0

        fig, ax = plt.subplots(figsize=(16, 8))

        cmap = plt.cm.colors.ListedColormap(["#81C784", "#FFF9C4", "#E0E0E0"])
        bounds = [-0.25, 0.25, 0.75, 1.25]
        norm = plt.cm.colors.BoundaryNorm(bounds, cmap.N)
        ax.imshow(color_matrix, cmap=cmap, norm=norm, aspect="auto")

        for i, row in enumerate(scan_matrix):
            for j, (before, after) in enumerate(row):
                if has_both and after and before != after:
                    text = f"{before}\n->\n{after}"
                elif has_both and after:
                    text = before
                else:
                    text = before
                ax.text(j, i, text, ha="center", va="center", fontsize=7,
                       fontweight="bold" if has_both and after and before != after else "normal")

        ax.set_xticks(range(len(databases)))
        ax.set_xticklabels([DB_LABELS[db] for db in databases], fontsize=11)
        ax.set_yticks(range(len(scenarios)))
        snames = []
        for sid in scenarios:
            entry = next((d for d in all_data if d["scenario_id"] == sid), {})
            snames.append(f"{sid}: {entry.get('scenario_name', '')}")
        ax.set_yticklabels(snames, fontsize=9)

        legend_elements = [
            Patch(facecolor="#81C784", label="Zmiana typu skanu (indeks pomaga)"),
            Patch(facecolor="#FFF9C4", label="Brak zmiany"),
            Patch(facecolor="#E0E0E0", label="Brak danych porownawczych"),
        ]
        if has_both:
            ax.legend(handles=legend_elements, loc="upper center",
                     bbox_to_anchor=(0.5, -0.05), ncol=3, fontsize=9)

        ax.set_title(f"EXPLAIN — zmiana typu skanu po dodaniu indeksow (wolumen {volume})",
                    fontsize=14, fontweight="bold")

        plt.tight_layout()
        path = self.output_dir / f"explain_scan_changes_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_explain_rows_reduction(self, all_data: list[dict], volume: str) -> Path:
        scenarios = sorted(set(d["scenario_id"] for d in all_data))
        databases = ["postgres", "mysql", "mongo", "neo4j"]

        lookup = {}
        for d in all_data:
            lookup[(d["scenario_id"], d["database"], d["index_label"])] = d

        has_both = any(d["index_label"] == "no_indexes" for d in all_data) and \
                   any(d["index_label"] == "with_indexes" for d in all_data)

        if not has_both:
            fig, ax = plt.subplots(figsize=(14, 7))
            x = np.arange(len(scenarios))
            width = 0.18
            for i, db in enumerate(databases):
                values = []
                for sid in scenarios:
                    entry = lookup.get((sid, db, "no_indexes"), {})
                    values.append(entry.get("rows_examined") or 0)
                ax.bar(x + i * width, values, width, label=DB_LABELS[db],
                      color=DB_COLORS[db])
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(scenarios)
            ax.set_ylabel("Przejrzane wiersze/dokumenty")
            ax.set_title(f"EXPLAIN — liczba przejrzanych wierszy (wolumen {volume})",
                        fontsize=14, fontweight="bold")
            ax.legend()
            ax.grid(axis="y", alpha=0.3)
            self._auto_log_scale(ax)
            plt.tight_layout()
            path = self.output_dir / f"explain_rows_examined_{volume}.png"
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            return path

        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

        for ax_idx, (ilabel, title) in enumerate([
            ("no_indexes", "Bez indeksow"),
            ("with_indexes", "Z indeksami"),
        ]):
            ax = axes[ax_idx]
            x = np.arange(len(scenarios))
            width = 0.18
            for i, db in enumerate(databases):
                values = []
                for sid in scenarios:
                    entry = lookup.get((sid, db, ilabel), {})
                    values.append(entry.get("rows_examined") or 0)
                bars = ax.bar(x + i * width, values, width, label=DB_LABELS[db],
                             color=DB_COLORS[db])
                for bar, val in zip(bars, values):
                    if val > 0:
                        if val >= 1_000_000:
                            txt = f"{val/1_000_000:.1f}M"
                        elif val >= 1_000:
                            txt = f"{val/1_000:.0f}K"
                        else:
                            txt = str(val)
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height(), txt,
                                ha="center", va="bottom", fontsize=6, rotation=45)
            ax.set_title(title, fontsize=12)
            ax.set_xticks(x + width * 1.5)
            ax.set_xticklabels(scenarios)
            ax.set_ylabel("Przejrzane wiersze / dokumenty / db_hits")
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=0.3)
            self._auto_log_scale(ax)

        fig.suptitle(
            f"EXPLAIN — liczba przejrzanych wierszy przed i po indeksach (wolumen {volume})",
            fontsize=14, fontweight="bold",
        )
        plt.tight_layout()
        path = self.output_dir / f"explain_rows_examined_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _chart_explain_exec_time(self, all_data: list[dict], volume: str) -> Path:
        time_dbs = ["postgres", "mongo"]
        scenarios = sorted(set(d["scenario_id"] for d in all_data))

        lookup = {}
        for d in all_data:
            lookup[(d["scenario_id"], d["database"], d["index_label"])] = d

        index_labels = sorted(set(d["index_label"] for d in all_data))
        has_both = len(index_labels) == 2

        if has_both:
            fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        else:
            fig, axes = plt.subplots(1, 1, figsize=(8, 6))
            axes = [axes]

        fig.suptitle(
            f"EXPLAIN — czas wykonania zapytan SELECT, wolumen {volume} (PostgreSQL vs MongoDB)",
            fontsize=14, fontweight="bold",
        )

        for ax, ilabel in zip(axes, index_labels):
            x = np.arange(len(scenarios))
            width = 0.3
            for i, db in enumerate(time_dbs):
                values = []
                for sid in scenarios:
                    entry = lookup.get((sid, db, ilabel), {})
                    values.append(entry.get("exec_time_ms") or 0)
                bars = ax.bar(x + i * width, values, width,
                             label=DB_LABELS[db], color=DB_COLORS[db])
                for bar, val in zip(bars, values):
                    if val > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height(), f"{val:.1f}",
                                ha="center", va="bottom", fontsize=7)

            title_label = "z indeksami" if "with" in ilabel else "bez indeksow"
            ax.set_title(title_label, fontsize=12)
            ax.set_xticks(x + width / 2)
            ax.set_xticklabels(scenarios)
            ax.set_ylabel("Czas wykonania [ms]")
            ax.legend(fontsize=9)
            ax.grid(axis="y", alpha=0.3)
            self._auto_log_scale(ax)

        plt.tight_layout()
        path = self.output_dir / f"explain_exec_time_{volume}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path
