```bash
docker compose up -d

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 main.py generate --volume small && python3 main.py load --volume small
```
