# TODO — Poprawki benchmarku ZTBD

**Cel:** porownywac bazy danych, nie drivery Pythona.

Wyniki dla small i medium juz zebrane. Ponizsze zmiany wymagaja ponownego uruchomienia benchmarkow.

---

## KRYTYCZNE — wplywaja na uczciwosci porownania

### 1. Sekwencyjne wykonanie baz (usunac ThreadPoolExecutor)

**Plik:** `app/src/benchmarks/runner.py:194-201`

**Problem:** `ThreadPoolExecutor` uruchamia wszystkie 4 bazy jednoczesnie dla kazdego scenariusza. Kontenery Docker dziela CPU, RAM i I/O — wyniki sa wzajemnie zaklocane.

**Ryzyko zepsucia:** Zerowe. Zamiana parallel na sequential jest mechanicznie prosta.

**Zmiana:** Zamienic `ThreadPoolExecutor` na zwykla petle `for`. Usunac `threading`, `results_lock`, `done_lock`, `print_lock` — przy sekwencyjnym wykonaniu nie sa potrzebne.

```python
for scenario in scenarios:
    for db_name in db_names:
        run_db(scenario, db_name)
```

---

### 2. MySQL bulk INSERT — multi-row INSERT zamiast `executemany`

**Pliki:** `app/src/benchmarks/insert_scenarios.py` — scenariusze I2, I4, I6
**Pliki polaczenia:** `app/src/benchmarks/runner.py:38,59` (jesli LOAD DATA)

**Problem:** PostgreSQL uzywa `COPY FROM STDIN` (strumieniowy transfer), MySQL uzywa `cur.executemany()` (parsowanie SQL per wiersz). Wynik: I2 medium PostgreSQL = 2.6s, MySQL = 157s (60x wolniejszy). To roznica protokolow ladowania, nie baz.

**Ryzyko zepsucia:** Niskie przy multi-row INSERT (nie wymaga konfiguracji serwera). Umiarkowane przy `LOAD DATA LOCAL INFILE` (wymaga `local_infile=True` w pymysql + `local_infile=ON` na serwerze MySQL + dostep do pliku tymczasowego).

**Zmiana (rekomendowana — multi-row INSERT):** Batche po 1000-5000 wierszy:
```python
batch_size = 5000
for i in range(0, len(self._data), batch_size):
    batch = self._data[i:i + batch_size]
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(batch))
    flat = [v for row in batch for v in row]
    cur.execute(f"INSERT INTO watch_history (...) VALUES {placeholders}", flat)
conn.commit()
```

Eliminuje overhead parsowania SQL per wiersz. Nie wymaga zmian konfiguracji serwera.

**Alternatywa (LOAD DATA):** Szybsza, ale wymaga:
- `pymysql.connect(..., local_infile=True)`
- MySQL server: `local_infile=ON` w docker-compose.yml
- Tymczasowy plik CSV (`tempfile.NamedTemporaryFile`)

---

### 3. I5 — PostgreSQL i MySQL uzywaja petli z pojedynczym INSERT

**Plik:** `app/src/benchmarks/insert_scenarios.py:573-600`

**Problem:**
```python
for d in self._data:
    conn.execute("INSERT INTO ratings ... VALUES ...")
```
MongoDB robi `insert_many` w batchach po 5000. SQL-e robia N oddzielnych round-tripow.

**Ryzyko zepsucia:** Zerowe. Semantyka identyczna — N insertow zamieniamy na batch.

**Zmiana:**
- PostgreSQL: `cur.executemany()` z psycopg (pod spodem robi pipelining) z `ON CONFLICT DO NOTHING`
- MySQL: multi-row `INSERT IGNORE INTO ratings ... VALUES (...), (...), ...` w batchach

Uwaga: `COPY` nie obsluguje `ON CONFLICT`, wiec dla PostgreSQL `executemany` jest lepszym wyborem niz COPY.

---

### 4. MongoDB U6 — petla `update_one` zamiast `bulk_write`

**Plik:** `app/src/benchmarks/update_scenarios.py:410-426`

**Problem:**
```python
for doc in db.content.find(...):
    db.content.update_one({"_id": cid}, {"$set": {"popularity_score": ...}})
```
~10K oddzielnych round-tripow sieciowych.

**Ryzyko zepsucia:** Zerowe. `bulk_write` wykonuje dokladnie te same operacje w jednym wywolaniu sieciowym.

**Zmiana:**
```python
from pymongo import UpdateOne
ops = []
for doc in db.content.find({}, {"total_views": 1, "avg_rating": 1}):
    cid = doc["_id"]
    score = (doc.get("total_views", 0) * 0.0001
             + doc.get("avg_rating", 0) * 5
             + ratings_counts.get(cid, 0) * 0.1)
    ops.append(UpdateOne({"_id": cid}, {"$set": {"popularity_score": round(score, 2)}}))
if ops:
    db.content.bulk_write(ops, ordered=False)
```

---

### 5. MongoDB I6 — ta sama petla `update_one`

**Plik:** `app/src/benchmarks/insert_scenarios.py:736-746`

**Problem:** identyczny jak U6 — N oddzielnych round-tripow do pushowania cast entries.

**Ryzyko zepsucia:** Zerowe. Analogiczna zamiana na `bulk_write`.

**Zmiana:**
```python
from pymongo import UpdateOne
ops = [
    UpdateOne(
        {"_id": r[0]},
        {"$push": {"cast": {
            "person_id": r[1],
            "first_name": f"BenchFirst{r[1] - self._start_id}",
            "last_name": f"BenchLast{r[1] - self._start_id}",
            "role": r[2], "character_name": r[3],
            "billing_order": r[4],
        }}},
    )
    for r in self._relations
]
if ops:
    db.content.bulk_write(ops, ordered=False)
```

---

### 6. Brak warmup — trial 1 jest outlierem

**Plik:** `app/src/benchmarks/runner.py:133`

**Problem:** Petla zaczyna sie od razu od trial 1. Przyklad cold start:
- Neo4j S1 medium: trial 1 = 180ms, trial 2 = 12ms, trial 3 = 10ms
- MySQL S1 medium: trial 1 = 526ms, trial 2 = 11ms, trial 3 = 11ms

Trial 1 mierzy: cache miss + JVM JIT (Neo4j) + plan cache miss. Srednia jest zdominowana.

**Ryzyko zepsucia:** Minimalne. Warmup wykonuje ten sam cykl setup-run-teardown co normalny trial, ale nie zapisuje wyniku. Teardown juz jest testowany w normalnych trialach.

**Zmiana:** Przed petla trials dodac 1 rozgrzewkowy cykl:
```python
try:
    sc.setup(db_name, conn, self.ctx)
    sc.run(db_name, conn, self.ctx)
    sc.teardown(db_name, conn, self.ctx)
except Exception:
    self._try_rollback(db_name, conn)
```

---

## WAZNE — jakosc analizy

### 7. Mediana zamiast (lub obok) sredniej

**Plik:** `app/src/analysis/visualizer.py:84-91`

**Problem:** Metoda `_avg()` uzywa `.mean()`. Srednia z [180, 12, 10] = 67ms. Mediana = 12ms. Mediana lepiej oddaje typowa wydajnosc.

**Ryzyko zepsucia:** Zerowe — zmiana wylacznie wizualizacji.

**Zmiana:** Dodac metode `_median()` analogiczna do `_avg()` (ale z `.median()`). Na wykresach uzyc mediany jako glownej wartosci + error bars (min-max). Zachowac `_avg()` jako alternatywna metryke.

---

### 8. Skala logarytmiczna na wykresach

**Pliki:** `app/src/analysis/visualizer.py` — metody `_chart_index_comparison`, `_chart_crud_by_database`

**Problem:** Na `index_comparison_medium` slupek U6 PostgreSQL (265s) zaglusza reszte scenariuszy — wiekszosc <100ms, os Y siega 265,000ms.

**Ryzyko zepsucia:** Zerowe.

**Zmiana:** Dodac `ax.set_yscale('log')` dla wykresow zbiorczych. Automatyczna detekcja: jesli `max/min > 100`, wlaczyc skale logarytmiczna.

---

### 9. MongoDB D1/D2 — opisac reczna kaskade w raporcie

**Plik:** `app/src/benchmarks/delete_scenarios.py:123-127`

**Problem:** PostgreSQL robi 1 operacje (`DELETE FROM content`) z CASCADE. MongoDB robi 4 oddzielne operacje (delete z watch_history, my_list, ratings, content). Czas MongoDB obejmuje 4 round-tripy.

**Zmiana w kodzie:** brak — to realna roznica architekturalna. **Opisac w sprawozdaniu** jako koszt modelu dokumentowego vs relacyjnego CASCADE.

---

### 10. Natywne daty w MongoDB i Neo4j

**Pliki:** `app/src/loaders/mongo_loader.py`, `app/src/loaders/neo4j_loader.py`, scenariusze S3, D3, teardowny I2, I5

**Problem:** `started_at` przechowywany jako string (`"2025-06-15 12:00:00"`). Porownania w S3/D3 (`{"$gte": "2025-06-01"}`) to porownania stringow, nie dat. Indeksy na stringach sa mniej efektywne niz na natywnych typach dat.

Uwaga: porownania leksykograficzne akurat dzialaja poprawnie dla formatu ISO, wiec wyniki sa poprawne — ale indeksy datowe nie sa wykorzystywane.

**Ryzyko zepsucia:** Umiarkowane. Zmiana typu dotyka wielu plikow:
1. Loadery — konwersja `datetime.fromisoformat()`
2. WSZYSTKIE scenariusze porownujace daty — przekazywac obiekty `datetime`, nie stringi
3. Teardowny tworzace dane z hardcodowanymi datami

**Zmiana:**
- MongoDB: konwertowac na `datetime` przy ladowaniu (`datetime.fromisoformat(...)`)
- Neo4j: uzyc `datetime()` w Cypher przy ladowaniu
- Dostosowac zapytania w scenariuszach do natywnych typow

**Rekomendacja:** Wdrozyc na koncu, po zmianach #1-6, ze wzgledu na wysoka inwazyjnosc.

---

### 11. D6 — filtr nie testuje tego co nazwa mowi

**Pliki:** `app/src/benchmarks/delete_scenarios.py:635-663` (wszystkie 4 bazy)

**Problem:** Nazwa: "Mass delete inactive users". Zapytanie:
```sql
DELETE FROM users WHERE user_id >= %s AND user_id < %s
```
To range scan na PK, nie filtrowanie po `status = 'deleted'`.

**Ryzyko zepsucia:** Zerowe. Setup juz tworzy uzytkownikow ze statusem `'deleted'` — wystarczy dodac warunek.

**Zmiana:** We wszystkich 4 bazach dodac filtr po statusie:
- PostgreSQL/MySQL: `DELETE FROM users WHERE status = 'deleted' AND user_id >= ... AND user_id < ...`
- MongoDB: `{"status": "deleted", "_id": {"$gte": ..., "$lt": ...}}`
- Neo4j: `WHERE u.status = 'deleted' AND u.user_id >= ... AND u.user_id < ...`

---

## DROBNE — poprawnosc teardown

### 12. I5 teardown Neo4j — zbyt szerokie kryterium usuwania

**Plik:** `app/src/benchmarks/insert_scenarios.py:667-671`

**Problem:**
```cypher
MATCH (p:Profile)-[r:RATED]->(c:Content)
WHERE r.created_at = '2025-06-15 12:00:00'
DELETE r
```
Usunie wszystkie relacje RATED z ta data, nie tylko dodane w benchmarku. **Aktualny kod juz psuje dane** — usuwajac oryginalne relacje z ta data.

**Ryzyko zepsucia:** Zerowe — poprawka naprawia istniejacy bug.

**Zmiana:** Filtrowac po `content_id = self._cid`:
```cypher
MATCH (p:Profile)-[r:RATED]->(c:Content {content_id: $cid})
DELETE r
```

---

### 13. I2 teardown Neo4j — ten sam problem

**Plik:** `app/src/benchmarks/insert_scenarios.py:243-247`

**Problem:** `WHERE w.started_at = '2025-06-15 12:00:00'` usunie wszystkie WATCHED z ta data. **Aktualny kod juz psuje dane.**

**Ryzyko zepsucia:** Zerowe — poprawka naprawia istniejacy bug.

**Zmiana:** Uzyc unikalnej daty benchmarkowej (np. `"2099-01-01 00:00:00"`) i ustawic ja w setup/run:
```python
_BENCH_DATE = "2099-01-01 00:00:00"
```
Wtedy teardown: `WHERE w.started_at = '2099-01-01 00:00:00'` nie trafi w oryginalne dane.

---

## Podsumowanie

### Tabela ryzyka i trudnosci

| #  | Zmiana                        | Ryzyko   | Trudnosc | Pliki                              |
|----|-------------------------------|----------|----------|------------------------------------|
| 1  | Sekwencyjne wykonanie         | Zerowe   | Niska    | `runner.py`                        |
| 6  | Warmup                        | Minimalne| Niska    | `runner.py`                        |
| 4  | MongoDB U6 bulk_write         | Zerowe   | Niska    | `update_scenarios.py`              |
| 5  | MongoDB I6 bulk_write         | Zerowe   | Niska    | `insert_scenarios.py`              |
| 3  | I5 batch INSERT               | Zerowe   | Niska    | `insert_scenarios.py`              |
| 2  | MySQL multi-row INSERT        | Niskie   | Srednia  | `insert_scenarios.py`, `runner.py` |
| 11 | D6 filtr status               | Zerowe   | Niska    | `delete_scenarios.py`              |
| 12 | I5 teardown Neo4j             | Zerowe   | Niska    | `insert_scenarios.py`              |
| 13 | I2 teardown Neo4j             | Zerowe   | Niska    | `insert_scenarios.py`              |
| 7  | Mediana + error bars          | Zerowe   | Srednia  | `visualizer.py`                    |
| 8  | Skala logarytmiczna           | Zerowe   | Niska    | `visualizer.py`                    |
| 9  | MongoDB D1/D2 opis            | —        | —        | Sprawozdanie (brak zmian w kodzie) |
| 10 | Natywne daty                  | Umiarkowane | Wysoka | loadery + scenariusze (wiele plikow) |

### Kolejnosc wdrazania

| Krok | Zadania | Efekt |
|------|---------|-------|
| 1    | #1 (sekwencyjne) + #6 (warmup) | Rzetelnosc pomiarow — 1 plik (`runner.py`) |
| 2    | #4-5 (MongoDB bulk_write) | Uczciwe porownanie MongoDB — 2 pliki |
| 3    | #3 (I5 batch) + #2 (MySQL multi-row) | Uczciwe porownanie bulk INSERT — 1 plik |
| 4    | #11 (D6 filtr) + #12-13 (teardown) | Poprawnosc semantyczna — 2 pliki |
| 5    | #7-8 (wykresy) | Jakosc wizualizacji — 1 plik |
| 6    | #10 (natywne daty) | Pelna poprawnosc typow — wiele plikow |

Po wdrozeniu krokow 1-4: **ponownie uruchomic benchmarki dla small i medium**.
Krok 5-6 mozna wdrozyc niezaleznie.
