```bash
docker compose up -d

python3 -m venv venv

Linux: source venv/bin/activate
Windows: .\venv\Scripts\Activate.ps1

pip install -r requirements.txt

Linux: python3 main.py generate --volume small && python3 main.py load --volume small
Windows: python main.py generate --volume small; python main.py load --volume small
```
