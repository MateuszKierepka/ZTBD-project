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

    for db in targets:
        if db == "postgres":
            from src.loaders.postgres_loader import PostgresLoader
            PostgresLoader(data_dir).load_all()
        elif db == "mysql":
            from src.loaders.mysql_loader import MySQLLoader
            MySQLLoader(data_dir).load_all()
        elif db == "mongo":
            from src.loaders.mongo_loader import MongoLoader
            MongoLoader(data_dir).load_all()
        elif db == "neo4j":
            from src.loaders.neo4j_loader import Neo4jLoader
            Neo4jLoader(data_dir).load_all()


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

    args = parser.parse_args()

    if args.command == "generate":
        cmd_generate(args)
    elif args.command == "load":
        cmd_load(args)


if __name__ == "__main__":
    main()
