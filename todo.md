# TODO — Poprawki benchmarku ZTBD

**Cel:** porownywac bazy danych, nie drivery Pythona.

Wyniki dla small i medium juz zebrane. Ponizsze zmiany wymagaja ponownego uruchomienia benchmarkow.

---

## KRYTYCZNE — wplywaja na uczciwosci porownania

### 1. Sekwencyjne wykonanie baz (usunac ThreadPoolExecutor)

**Plik:** `app/src/benchmarks/runner.py:194-201`

**Problem:** `ThreadPoolExecutor` uruchamia wszystkie 4 bazy jednoczesnie dla kazdego scenariusza. Kontenery Docker dziela CPU, RAM i I/O — wyniki sa wzajemnie zaklocane.

**Zmiana:** iteracja po bazach sekwencyjnie, jedna po drugiej.

---

### 2. MySQL bulk INSERT — `LOAD DATA` zamiast `executemany`

**Pliki:** `app/src/benchmarks/insert_scenarios.py` — scenariusze I2, I4, I6

**Problem:** PostgreSQL uzywa `COPY FROM STDIN` (strumieniowy transfer), MySQL uzywa `cur.executemany()` (parsowanie SQL per wiersz). Wynik: I2 medium PostgreSQL = 2.6s, MySQL = 157s (60x wolniejszy). To roznica protokolow ladowania, nie baz.

**Zmiana:** uzyc `LOAD DATA LOCAL INFILE` dla MySQL:
- `pymysql.connect(..., local_infile=True)`
- zapis danych do tymczasowego pliku CSV (`tempfile.NamedTemporaryFile`)
- `LOAD DATA LOCAL INFILE '/tmp/file.csv' INTO TABLE ...`

Alternatywnie: multi-row INSERT z batchami po 1000-5000 wierszy (`INSERT INTO t VALUES (...), (...), ...`).

---

### 3. I5 — PostgreSQL i MySQL uzywaja petli z pojedynczym INSERT

**Plik:** `app/src/benchmarks/insert_scenarios.py:573-600`

**Problem:**
```python
for d in self._data:
    conn.execute("INSERT INTO ratings ... VALUES ...")
```
MongoDB robi `insert_many` w batchach po 5000. SQL-e robia N oddzielnych round-tripow.

**Zmiana:** PostgreSQL → `COPY`, MySQL → `executemany` lub multi-row INSERT.

---

### 4. MongoDB U6 — petla `update_one` zamiast `bulk_write`

**Plik:** `app/src/benchmarks/update_scenarios.py:410-426`

**Problem:**
```python
for doc in db.content.find(...):
    db.content.update_one({"_id": cid}, {"$set": {"popularity_score": ...}})
```
~10K oddzielnych round-tripow sieciowych.

**Zmiana:**
```python
from pymongo import UpdateOne
ops = []
for doc in db.content.find(...):
    ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"popularity_score": score}}))
db.content.bulk_write(ops, ordered=False)
```

---

### 5. MongoDB I6 — ta sama petla `update_one`

**Plik:** `app/src/benchmarks/insert_scenarios.py:736-746`

**Problem:** identyczny jak U6 — N oddzielnych round-tripow do pushowania cast entries.

**Zmiana:** `bulk_write` z lista `UpdateOne`.

---

### 6. Brak warmup — trial 1 jest outlierem

**Plik:** `app/src/benchmarks/runner.py:133`

**Problem:** Petla zaczyna sie od razu od trial 1. Przyklad cold start:
- Neo4j S1 medium: trial 1 = 180ms, trial 2 = 12ms, trial 3 = 10ms
- MySQL S1 medium: trial 1 = 526ms, trial 2 = 11ms, trial 3 = 11ms

Trial 1 mierzy: cache miss + JVM JIT (Neo4j) + plan cache miss. Srednia jest zdominowana.

**Zmiana:** przed petla trials dodac 1 rozgrzewkowy cykl (setup → run → teardown) bez zapisywania wyniku. Alternatywnie: zwiekszyc trials do 5 i odrzucic najgorszy wynik.

---

## WAZNE — jakosc analizy

### 7. Mediana zamiast (lub obok) sredniej

**Problem:** Srednia z [180, 12, 10] = 67ms. Mediana = 12ms. Mediana lepiej oddaje typowa wydajnosc.

**Zmiana:** w wizualizacji uzyc mediany jako glownej metryki. Dodac error bars (min-max lub stddev) na wykresach slupkowych.

---

### 8. Skala logarytmiczna na wykresach

**Problem:** Na `index_comparison_medium` slupek U6 PostgreSQL (265s) zaglusza reszte scenariuszy — wiekszosc <100ms, os Y siega 265,000ms.

**Zmiana:** dodac `ax.set_yscale('log')` dla wykresow zbiorczych. Albo rozdzielic scenariusze na "lekkie" (<1s) i "ciezkie" (>1s) na osobnych panelach.

---

### 9. MongoDB D1/D2 — opisac reczna kaskade w raporcie

**Plik:** `app/src/benchmarks/delete_scenarios.py:123-127`

**Problem:** PostgreSQL robi 1 operacje (`DELETE FROM content`) z CASCADE. MongoDB robi 4 oddzielne operacje (delete z watch_history, my_list, ratings, content). Czas MongoDB obejmuje 4 round-tripy.

**Zmiana w kodzie:** brak — to realna roznica architekturalna. **Opisac w sprawozdaniu** jako koszt modelu dokumentowego.

---

### 10. Natywne daty w MongoDB i Neo4j

**Pliki:** `app/src/loaders/mongo_loader.py`, `app/src/loaders/neo4j_loader.py`

**Problem:** `started_at` przechowywany jako string (`"2025-06-15 12:00:00"`). Porownania w S3/D3 (`{"$gte": "2025-06-01"}`) to porownania stringow, nie dat. Indeksy na stringach sa mniej efektywne niz na natywnych typach dat.

**Zmiana:**
- MongoDB: konwertowac na `datetime` przy ladowaniu (`datetime.fromisoformat(...)`)
- Neo4j: uzyc `datetime()` w Cypher przy ladowaniu
- Dostosowac zapytania w scenariuszach do natywnych typow

---

### 11. D6 — filtr nie testuje tego co nazwa mowi

**Pliki:** `app/src/benchmarks/delete_scenarios.py:635-640`

**Problem:** Nazwa: "Mass delete inactive users". Zapytanie:
```sql
DELETE FROM users WHERE user_id >= %s AND user_id < %s
```
To range scan na PK, nie filtrowanie po `status = 'deleted'`.

**Zmiana:** `DELETE FROM users WHERE status = 'deleted' AND user_id >= ... AND user_id < ...`

---

## DROBNE — poprawnosc teardown

### 12. I5 teardown Neo4j — zbyt szerokie kryterium usuwania

**Plik:** `app/src/benchmarks/insert_scenarios.py:668-671`

**Problem:**
```cypher
MATCH (p:Profile)-[r:RATED]->(c:Content)
WHERE r.created_at = '2025-06-15 12:00:00'
DELETE r
```
Usunie wszystkie relacje RATED z ta data, nie tylko dodane w benchmarku. Moze usunac oryginalne dane.

**Zmiana:** filtrowac po `content_id = self._cid` albo uzyc unikalnej daty per trial.

---

### 13. I2 teardown Neo4j — ten sam problem

**Plik:** `app/src/benchmarks/insert_scenarios.py:243-247`

**Problem:** `WHERE w.started_at = '2025-06-15 12:00:00'` usunie wszystkie WATCHED z ta data.

**Zmiana:** uzyc unikalnej daty per trial lub filtrowac po zakresie ID.

---

## Podsumowanie priorytetu implementacji

| Priorytet | Zadania | Efekt |
|-----------|---------|-------|
| Najpierw  | #1 (sekwencyjne), #6 (warmup) | Rzetelnosc pomiarow |
| Potem     | #2 (MySQL LOAD DATA), #3 (I5 batch), #4-5 (MongoDB bulk_write) | Uczciwe porownanie bulk operacji |
| Na koniec | #7-8 (wykresy), #9-11 (raport), #12-13 (teardown) | Jakosc prezentacji i poprawnosc |

Po wdrozeniu zmian #1-6: **ponownie uruchomic benchmarki dla small i medium**.
