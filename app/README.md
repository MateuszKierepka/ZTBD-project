```bash
docker compose up -d
```

```bash
python3 -m venv venv
```

```bash
Linux: source venv/bin/activate
Windows: .\venv\Scripts\Activate.ps1
```

```bash
pip install -r requirements.txt
```

# Full pipeline (single command per volume)
```bash
python main.py run-all --volume [volume-name] --trials 3
```

# If data is already generated:
```bash
python main.py run-all --volume [volume-name] --trials 3 --skip-generate
```

# Or run each step manually

## Small
```bash
python main.py generate --volume small --seed 42
python main.py load --volume small --no-indexes
python main.py benchmark --volume small --trials 3
python main.py explain --volume small
python main.py load --volume small
python main.py benchmark --volume small --trials 3 --with-indexes
python main.py explain --volume small --with-indexes
```

## Medium
```bash
python main.py generate --volume medium --seed 42
python main.py load --volume medium --no-indexes
python main.py benchmark --volume medium --trials 3
python main.py explain --volume medium
python main.py load --volume medium
python main.py benchmark --volume medium --trials 3 --with-indexes
python main.py explain --volume medium --with-indexes
```

## Large
```bash
python main.py generate --volume large --seed 42
python main.py load --volume large --no-indexes
python main.py benchmark --volume large --trials 3
python main.py explain --volume large
python main.py load --volume large
python main.py benchmark --volume large --trials 3 --with-indexes
python main.py explain --volume large --with-indexes
```

```bash
python main.py visualize
```
