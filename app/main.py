#!/usr/bin/env python3
import argparse
from pathlib import Path

from src.config import VOLUMES


def cmd_generate(args: argparse.Namespace) -> None:
    from src.generators.data_generator import DataGenerator

    config = VOLUMES[args.volume]
    output_dir = args.data_dir / config.name

    generator = DataGenerator(config=config, output_dir=output_dir, seed=args.seed)
    generator.generate_all()


def cmd_load(args: argparse.Namespace) -> None:
    data_dir = args.data_dir / args.volume

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        print(f"Run 'python main.py generate --volume {args.volume}' first.")
        return

    targets = (
        ["postgres", "mysql", "mongo", "neo4j"]
        if args.database == "all"
        else [args.database]
    )

    no_indexes = getattr(args, "no_indexes", False)

    for db in targets:
        if db == "postgres":
            from src.loaders.postgres_loader import PostgresLoader
            loader = PostgresLoader(data_dir)
            loader.load_all()
            if no_indexes:
                loader.drop_indexes()
            else:
                loader.create_indexes()
        elif db == "mysql":
            from src.loaders.mysql_loader import MySQLLoader
            loader = MySQLLoader(data_dir)
            loader.load_all()
            if no_indexes:
                loader.drop_indexes()
            else:
                loader.create_indexes()
        elif db == "mongo":
            from src.loaders.mongo_loader import MongoLoader
            loader = MongoLoader(data_dir)
            loader.load_all()
            if not no_indexes:
                loader.create_indexes()
            loader.close()
        elif db == "neo4j":
            from src.loaders.neo4j_loader import Neo4jLoader
            loader = Neo4jLoader(data_dir)
            loader.load_all()
            if not no_indexes:
                loader.create_indexes()
            loader.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="VOD Platform — ZTBD Project")
    subparsers = parser.add_subparsers(dest="command", required=True)

    gen = subparsers.add_parser("generate", help="Generate test data to CSV files")
    gen.add_argument(
        "--volume",
        choices=VOLUMES.keys(),
        default="small",
        help="Data volume size (default: small)",
    )
    gen.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Output base directory (default: data/)",
    )
    gen.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )

    load = subparsers.add_parser("load", help="Load CSV data into databases")
    load.add_argument(
        "--volume",
        choices=VOLUMES.keys(),
        default="small",
        help="Data volume to load (default: small)",
    )
    load.add_argument(
        "--database",
        choices=["all", "postgres", "mysql", "mongo", "neo4j"],
        default="all",
        help="Target database (default: all)",
    )
    load.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Base directory with generated CSV data (default: data/)",
    )
    load.add_argument(
        "--no-indexes",
        action="store_true",
        help="Skip index creation for MongoDB and Neo4j (for benchmarking without indexes)",
    )

    bench = subparsers.add_parser("benchmark", help="Run CRUD benchmarks")
    bench.add_argument(
        "--volume",
        choices=VOLUMES.keys(),
        default="small",
        help="Data volume to benchmark (default: small)",
    )
    bench.add_argument(
        "--database",
        choices=["all", "postgres", "mysql", "mongo", "neo4j"],
        default="all",
        help="Target database (default: all)",
    )
    bench.add_argument(
        "--scenarios",
        type=str,
        default=None,
        help="Comma-separated scenario IDs to run (e.g., S1,S2,I1). Default: all",
    )
    bench.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Number of trials per scenario (default: 3)",
    )
    bench.add_argument(
        "--with-indexes",
        action="store_true",
        help="Label results as 'with indexes'",
    )
    bench.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Directory for benchmark results (default: results/)",
    )

    exp = subparsers.add_parser("explain", help="Run EXPLAIN analysis on SELECT scenarios")
    exp.add_argument(
        "--volume",
        choices=VOLUMES.keys(),
        default="small",
        help="Data volume context (default: small)",
    )
    exp.add_argument(
        "--with-indexes",
        action="store_true",
        help="Label results as 'with indexes'",
    )
    exp.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Directory for EXPLAIN results (default: results/)",
    )

    run_all = subparsers.add_parser(
        "run-all",
        help="Full pipeline: load -> benchmark -> explain (both index variants)",
    )
    run_all.add_argument(
        "--volume",
        choices=VOLUMES.keys(),
        default="small",
        help="Data volume (default: small)",
    )
    run_all.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Number of trials per scenario (default: 3)",
    )
    run_all.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Base directory with generated CSV data (default: data/)",
    )
    run_all.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Directory for results (default: results/)",
    )
    run_all.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip data generation (use existing data)",
    )

    vis = subparsers.add_parser("visualize", help="Generate charts from benchmark results")
    vis.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Directory with benchmark CSV files (default: results/)",
    )
    vis.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results/charts"),
        help="Directory for generated charts (default: results/charts/)",
    )

    args = parser.parse_args()

    if args.command == "generate":
        cmd_generate(args)
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "benchmark":
        cmd_benchmark(args)
    elif args.command == "explain":
        cmd_explain(args)
    elif args.command == "visualize":
        cmd_visualize(args)
    elif args.command == "run-all":
        cmd_run_all(args)


def cmd_benchmark(args: argparse.Namespace) -> None:
    from src.benchmarks.runner import BenchmarkRunner

    databases = (
        ["postgres", "mysql", "mongo", "neo4j"]
        if args.database == "all"
        else [args.database]
    )
    scenario_ids = args.scenarios.split(",") if args.scenarios else None
    label = "with_indexes" if args.with_indexes else "no_indexes"

    runner = BenchmarkRunner(volume=args.volume, results_dir=args.results_dir)
    runner.connect(databases)
    runner.build_context()

    print(f"Running benchmarks: volume={args.volume}, "
          f"databases={databases}, indexes={label}")
    results = runner.run_all(
        with_indexes=args.with_indexes,
        trials=args.trials,
        scenario_ids=scenario_ids,
    )

    filename = f"benchmark_{args.volume}_{label}.csv"
    runner.save_results(results, filename)
    runner.close()


def cmd_explain(args: argparse.Namespace) -> None:
    from src.benchmarks.explain_analyzer import ExplainAnalyzer

    label = "with_indexes" if args.with_indexes else "no_indexes"

    analyzer = ExplainAnalyzer(results_dir=args.results_dir)
    analyzer.connect()

    print(f"Running EXPLAIN analysis: volume={args.volume}, indexes={label}")
    analyzer.analyze_all(volume=args.volume, with_indexes=args.with_indexes)
    analyzer.close()


def cmd_visualize(args: argparse.Namespace) -> None:
    from src.analysis.visualizer import Visualizer

    viz = Visualizer(results_dir=args.results_dir, output_dir=args.output_dir)
    viz.load_results()
    files = viz.generate_all()
    for f in files:
        print(f"  -> {f}")


def cmd_run_all(args: argparse.Namespace) -> None:
    import time as _time

    volume = args.volume
    trials = args.trials
    data_dir = args.data_dir
    results_dir = args.results_dir

    steps = [
        f"generate --volume {volume}",
        f"load --no-indexes --volume {volume}",
        f"benchmark --volume {volume} --trials {trials}",
        f"explain --volume {volume}",
        f"load --volume {volume}",
        f"benchmark --volume {volume} --trials {trials} --with-indexes",
        f"explain --volume {volume} --with-indexes",
        "visualize",
    ]

    if args.skip_generate:
        steps = steps[1:]

    total_start = _time.perf_counter()
    print(f"{'='*60}")
    print(f"  RUN-ALL PIPELINE: volume={volume}, trials={trials}")
    print(f"{'='*60}")

    for i, step_desc in enumerate(steps, 1):
        print(f"\n{'─'*60}")
        print(f"  Step {i}/{len(steps)}: python main.py {step_desc}")
        print(f"{'─'*60}\n")

        step_start = _time.perf_counter()
        parts = step_desc.split()
        cmd = parts[0]

        if cmd == "generate":
            ns = argparse.Namespace(
                volume=volume, data_dir=data_dir, seed=42,
            )
            cmd_generate(ns)

        elif cmd == "load":
            no_indexes = "--no-indexes" in parts
            ns = argparse.Namespace(
                volume=volume, database="all",
                data_dir=data_dir, no_indexes=no_indexes,
            )
            cmd_load(ns)

        elif cmd == "benchmark":
            with_indexes = "--with-indexes" in parts
            ns = argparse.Namespace(
                volume=volume, database="all", scenarios=None,
                trials=trials, with_indexes=with_indexes,
                results_dir=results_dir,
            )
            cmd_benchmark(ns)

        elif cmd == "explain":
            with_indexes = "--with-indexes" in parts
            ns = argparse.Namespace(
                volume=volume, with_indexes=with_indexes,
                results_dir=results_dir,
            )
            cmd_explain(ns)

        elif cmd == "visualize":
            ns = argparse.Namespace(
                results_dir=results_dir,
                output_dir=results_dir / "charts",
            )
            cmd_visualize(ns)

        elapsed = _time.perf_counter() - step_start
        print(f"\n  Step {i} completed in {elapsed:.1f}s")

    total = _time.perf_counter() - total_start
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE — total time: {total:.1f}s ({total/60:.1f}min)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
