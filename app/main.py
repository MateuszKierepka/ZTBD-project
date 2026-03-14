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
            PostgresLoader(data_dir).load_all()
        elif db == "mysql":
            from src.loaders.mysql_loader import MySQLLoader
            MySQLLoader(data_dir).load_all()
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

    args = parser.parse_args()

    if args.command == "generate":
        cmd_generate(args)
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "benchmark":
        cmd_benchmark(args)


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


if __name__ == "__main__":
    main()
