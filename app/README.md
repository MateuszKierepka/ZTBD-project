```bash
docker compose up -d

python3 -m venv venv

Linux: source venv/bin/activate
Windows: .\venv\Scripts\Activate.ps1

pip install -r requirements.txt

python main.py generate --volume small

python main.py load --volume small --no-indexes
python main.py benchmark --volume small

python main.py load --volume small
python main.py benchmark --volume small --with-indexes
```
