# Omówienie postępów projektu ZTBD — Platforma VOD

**Temat:** Porównanie wydajności 4 SZBD (PostgreSQL, MySQL, MongoDB, Neo4j)

---

## Co zostało zrealizowane od ostatniej konsultacji

Na poprzednich zajęciach omówione zostały **generatory danych** (Faker) oraz **loadery** do baz. Od tamtego momentu zrealizowano:

1. **System indeksów** dla wszystkich 4 baz danych (z możliwością uruchamiania testów BEZ i Z indeksami)
2. **Analizator EXPLAIN** — automatyczne zbieranie planów zapytań z 4 silników
3. **Wizualizator wyników** — generowanie wykresów matplotlib (grouped bar charts, heatmapy, porównania)
4. **Pipeline run-all** — jedno polecenie uruchamia cały cykl: load → benchmark bez indeksów → EXPLAIN → load z indeksami → benchmark z indeksami → EXPLAIN → wizualizacja
5. **Kolumna metadata (JSONB/JSON)** — dane półustrukturalne w tabeli content

Benchmarki zostały już wykonane dla wolumenów **small** i **medium** (zarówno bez jak i z indeksami).

---

## Architektura benchmarków

### Klasa bazowa (`base.py`)

`BaseScenario` — każdy scenariusz implementuje wzorzec **setup → run → teardown** per baza:

- `setup(db_type, conn, ctx)` — przygotowanie danych testowych (np. wstawienie rekordu do usunięcia)
- `run(db_type, conn, ctx)` — **właściwa operacja, która jest mierzona** (`time.perf_counter_ns()`)
- `teardown(db_type, conn, ctx)` — sprzątanie, przywrócenie stanu bazy

Każdy scenariusz ma **4 implementacje** (run_postgres, run_mysql, run_mongo, run_neo4j). Gdy runner wywołuje `run("postgres", ...)`, klasa bazowa automatycznie wybiera odpowiednią metodę `run_postgres()` — nie trzeba pisać if/elif na typ bazy.

`BenchmarkContext` — dostarcza:
- `max_ids` — wartości MAX(id) per tabela (żeby nie kolidować z istniejącymi danymi)
- `test_id(table)` — generuje unikalne ID dla danych testowych (max + 100_000 + counter)
- `random_id(table)` — losowy istniejący ID (do SELECT/UPDATE)
- `params` — parametry zależne od wolumenu (np. batch_watch_history: 10K/100K/500K)

### Runner (`runner.py`)

`BenchmarkRunner` — orkiestracja:
1. `connect(databases)` — otwiera połączenia do wybranych baz
2. `build_context()` — pobiera MAX(id) z PostgreSQL dla wszystkich tabel
3. **Dla każdego scenariusza × każdej bazy:**
   - Tworzy **NOWE połączenie** (nowa sesja TCP = czysty stan sesji)
   - `_flush_caches()` — czyszczenie cache na poziomie silnika (szczegóły niżej)
   - Pętla 3 triali: `setup → run (mierzony) → teardown`
   - Zamknięcie połączenia
4. `save_results()` — zapis do CSV: `scenario_id, scenario_name, category, database, volume, trial, time_ms, with_indexes`

### Cold start — brak warm-upów

Przed scenariuszem **NIE wykonujemy rozgrzewki**. Trial 1 zawsze startuje "na zimno":
- Trial 1 jest naturalnie wolniejszy (cold start) — cache silnika jest pusty, plany zapytań nie są skompilowane
- Trialy 2 i 3 korzystają z "rozgrzanego" cache (buffer pool, plan cache) — są szybsze
- Pozwala to zaobserwować **wpływ cache'owania silnika bazodanowego na wydajność**
- Przykładowy wzorzec: `S1 | postgres | avg=45.2ms (82.3, 30.1, 23.2)` — trial 1 wyraźnie wolniejszy

### Czyszczenie cache między scenariuszami

Przed każdym scenariuszem `_flush_caches()` czyści cache na poziomie silnika, żeby każdy scenariusz startował z czystym stanem:

| Baza | Komenda | Co czyści |
|------|---------|-----------|
| **PostgreSQL** | `DISCARD ALL` | Prepared statements, parametry sesji, tymczasowe tabele |
| **MySQL** | `FLUSH TABLES` | Zamyka i wymusza ponowne otwarcie plików tabel |
| **MongoDB** | `planCacheClear` na 5 kolekcjach | Cache planów zapytań (query plan cache) |
| **Neo4j** | `CALL db.clearQueryCaches()` | Cache planów zapytań Cypher |

**Ograniczenie:** Cache na poziomie buffer pool (PostgreSQL shared_buffers, InnoDB buffer pool, WiredTiger cache, Neo4j page cache) nie może być czyszczony bez restartu serwera. To ograniczenie systemowe — niezależne od aplikacji benchmarkowej.

### Dlaczego takie podejście?

- **Nowe połączenie per scenariusz+baza** — izolacja sesji, brak wpływu stanu poprzedniego scenariusza
- **Flush caches** — czyszczenie cache planów zapytań, żeby każdy scenariusz zaczynał od czystego stanu silnika
- **Brak warm-upów** — celowo zachowujemy efekt cold start na trialu 1 (widoczny wpływ cache)
- **Setup/teardown** — test nie zmienia stanu bazy, dane po benchmarku są takie same jak przed
- **perf_counter_ns()** — najdokładniejszy timer w Pythonie (nanosekundowa precyzja)
- **Mierzone jest TYLKO `run()`** — setup i teardown nie wchodzą w czas

---

## 24 scenariusze testowe — szczegóły

### INSERT (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **I1** | Rejestracja użytkownika | Wstawienie danych do 3 tabel: users + profiles + subscriptions |
| **I2** | Bulk import watch_history | Masowe wstawienie N rekordów (10K–500K zależnie od wolumenu) |
| **I3** | Dodanie serialu z pełnym drzewem | Wstawienie content + 3 sezony × 10 odcinków + 10 aktorów |
| **I4** | Batch insert płatności | Masowe wstawienie N rekordów do payments (1K–50K) |
| **I5** | Oceny z przeliczeniem avg_rating | Masowe wstawienie ocen + przeliczenie średniej oceny na content |
| **I6** | Import osób z powiązaniami | Masowe wstawienie osób (people) + przypisanie ich do treści (content_people) |

**Jak różni się implementacja między bazami (INSERT):**
- **PG** — masowe operacje przez `COPY FROM STDIN` (natywny bulk-load PostgreSQL, najszybszy sposób), pojedyncze przez zwykłe INSERT z `RETURNING user_id` żeby dostać ID nowego rekordu
- **MySQL** — masowe przez batch INSERT (wstawianie wielu wierszy jednym zapytaniem, po 5000 naraz), ID nowego rekordu przez `lastrowid`
- **MongoDB** — masowe przez `insert_many()` (wrzuca listę dokumentów naraz), w I1 cały user to jeden dokument z zagnieżdżonymi profilami i subskrypcją (nie ma osobnych tabel), w I6 zamiast osobnej tabeli content_people dodajemy aktorów do tablicy `cast` wewnątrz dokumentu content
- **Neo4j** — masowe przez `UNWIND` (przekazujemy listę danych jako parametr, baza iteruje po niej), tworzy węzły i relacje między nimi, np. `(Profile)-[:WATCHED]->(Content)`

### SELECT (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **S1** | Strona główna | Filtrowanie treści po gatunku "Action", sortowanie po popularności, zwrócenie 20 najlepszych + odczyt pola JSON (studio) |
| **S2** | Collaborative filtering | Znajdź użytkowników o podobnych gustach (oglądali to samo), polecaj treści których nasz user jeszcze nie widział |
| **S3** | TOP 100 oglądalności | Policz ile razy każda treść była oglądana w ostatnim miesiącu, zwróć 100 najpopularniejszych |
| **S4** | Full-text search po tytule | Wyszukiwanie treści po fragmencie tytułu + odczyt pola JSON (tags) |
| **S5** | Historia oglądania profilu | 50 ostatnich pozycji z historii oglądania danego profilu, razem z tytułami treści |
| **S6** | Filmografia osoby | Wszystkie treści w których dana osoba brała udział (jako aktor/reżyser/scenarzysta) |

**Jak różni się implementacja między bazami (SELECT):**
- **PG/MySQL** — klasyczne JOIN-y między tabelami, WHERE do filtrowania, GROUP BY do agregacji. W S1 dostęp do pola JSON przez `metadata->>'studio'` (PG) / `metadata->>'$.studio'` (MySQL). W S4 wyszukiwanie przez `ILIKE '%term%'` (PG) / `LIKE '%term%'` (MySQL)
- **MongoDB** — zamiast JOIN-ów używa `aggregate` pipeline (łańcuch operacji: filtruj → grupuj → sortuj → dołącz dane z innej kolekcji przez `$lookup`). W S4 wyszukiwanie przez wbudowany text search (`$text`). W S6 wystarczy jedno zapytanie bo aktorzy są zagnieżdżeni w dokumencie content
- **Neo4j** — zapytania Cypher oparte na przechodzeniu po grafie, np. "znajdź profil, przejdź po relacji WATCHED do treści, przejdź dalej do innych profili które też to oglądały". Naturalnie modeluje rekomendacje (S2) — graf jest do tego stworzony

### UPDATE (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **U1** | Aktualizacja postępu | Zmiana procentu obejrzenia w historii oglądania |
| **U2** | Przeliczenie avg_rating | Obliczenie średniej oceny ze wszystkich recenzji i zapisanie jej na treści |
| **U3** | Masowa zmiana planu | Zmiana planu subskrypcji z "Basic" na "Standard" dla wszystkich aktywnych użytkowników |
| **U4** | Aktualizacja danych usera | Zmiana emaila i telefonu użytkownika |
| **U5** | Oznaczenie treści jako nieaktywnej | Ustawienie flagi is_active = false na wybranej treści |
| **U6** | Masowa aktualizacja popularity_score | Przeliczenie wyniku popularności dla WSZYSTKICH treści wg formuły (views × 0.0001 + avg_rating × 5 + liczba_ocen × 0.1) |

**Jak różni się implementacja między bazami (UPDATE):**
- **PG/MySQL** — klasyczny UPDATE z WHERE, w U2 i U6 podzapytanie do obliczenia wartości (np. `SET avg_rating = (SELECT AVG(score) FROM ratings WHERE ...)`)
- **MongoDB** — `update_one` / `update_many` na dokumentach. W U3 subskrypcja jest zagnieżdżona w dokumencie usera, więc aktualizujemy pole `subscription.plan_name` wewnątrz dokumentu. W U6 trzeba najpierw policzyć oceny osobnym zapytaniem, a potem zrobić masową aktualizację
- **Neo4j** — `MATCH` + `SET` na węzłach/relacjach. W U2 oblicza średnią z relacji `RATED` i ustawia ją na węźle Content

### DELETE (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **D1** | Usunięcie treści z kaskadą | Usunięcie treści razem ze wszystkimi powiązanymi danymi (sezony, odcinki, historia, lista, oceny) |
| **D2** | Usunięcie profilu z historią | Usunięcie profilu razem z jego historią oglądania, listą i ocenami |
| **D3** | Czyszczenie starej historii | Usunięcie wszystkich wpisów historii oglądania starszych niż rok |
| **D4** | Usunięcie z my_list | Usunięcie jednej pozycji z listy "do obejrzenia" |
| **D5** | Usunięcie subskrypcji z płatnościami | Usunięcie subskrypcji razem z jej płatnościami |
| **D6** | Masowe usunięcie nieaktywnych userów | Usunięcie wszystkich użytkowników ze statusem "deleted" |

**Jak różni się implementacja między bazami (DELETE):**
- **PG/MySQL** — kaskadowe usuwanie działa automatycznie dzięki `ON DELETE CASCADE` na kluczach obcych — wystarczy usunąć rodzica (np. content), a baza sama usunie sezony, odcinki, historię itp.
- **MongoDB** — nie ma CASCADE, trzeba ręcznie usuwać z każdej kolekcji osobno (np. w D1: usuń z watch_history, my_list, ratings, i dopiero potem z content). W D2 profil jest zagnieżdżony w userze, więc usuwamy go operacją `$pull` (wyciągnięcie elementu z tablicy)
- **Neo4j** — `DETACH DELETE` usuwa węzeł razem ze wszystkimi jego relacjami. W D3 usuwanie batchowane (po 10000 naraz w pętli), bo usunięcie milionów relacji na raz mogłoby spowodować brak pamięci

---

## System indeksów

Indeksy tworzone są **po załadowaniu danych** przez metodę `create_indexes()` w każdym loaderze. Pipeline:

1. `load --no-indexes` → ładowanie danych BEZ indeksów dodatkowych (tylko PK i FK)
2. **Benchmark BEZ indeksów** → wyniki baseline
3. `load` (domyślnie Z indeksami) → tworzenie indeksów
4. **Benchmark Z indeksami** → wyniki po optymalizacji
5. Porównanie (heatmapa before/after)

### PostgreSQL (17 indeksów)
- B-tree: status, user_id, subscription_id, popularity_score, started_at, content_id, person_id
- **GIN trigram**: `idx_content_title_trgm` (na title, do ILIKE — wymaga `pg_trgm`)
- **GIN JSONB**: `idx_content_metadata_gin` (na metadata)
- **Partial index**: `idx_content_active_popular` (popularity_score DESC WHERE is_active = TRUE)
- Composite: `idx_wh_profile_started (profile_id, started_at DESC)`

### MySQL (10 indeksów)
- B-tree: status, popularity_score, is_active, started_at
- **FULLTEXT**: `idx_content_title_ft` (na title)
- **Functional index**: `idx_content_metadata_studio (CAST(metadata->>'$.studio' AS CHAR(100)))` (indeks na polu JSON)
- Composite: `idx_wh_profile_started (profile_id, started_at DESC)`

### MongoDB (13 indeksów)
- Single field: email (unique), status, type, genres, popularity_score, is_active, content_id, subscription_id, status
- **Text index**: na title (do `$text search`)
- Compound: `(profile_id, started_at DESC)`, `(profile_id, content_id)` unique
- Compound unique: `(profile_id, content_id)` na ratings i my_list

### Neo4j (9 indeksów)
- Property (RANGE): status (User, Subscription, Payment), type, popularity_score, is_active (Content)
- **TEXT index**: na title i metadata (Content)
- Country_code (User)

---

## EXPLAIN Analyzer

Klasa `ExplainAnalyzer` zbiera plany zapytań dla 6 scenariuszy SELECT ze wszystkich 4 baz:

| Baza | Metoda | Format |
|------|--------|--------|
| **PostgreSQL** | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` | JSON z Planning Time, Execution Time, scan types |
| **MySQL** | `EXPLAIN FORMAT=JSON` | JSON z access_type, key, rows_examined |
| **MongoDB** | `.explain("executionStats")` | nReturned, totalDocsExamined, totalKeysExamined, scan_type (COLLSCAN vs IXSCAN) |
| **Neo4j** | `PROFILE` prefix | db_hits, rows, operator types |

Wyniki zapisywane do `results/explain/explain_{volume}_{label}.json`. Dla każdego scenariusza ekstrahowane jest podsumowanie:
- PG: execution_time_ms, scan_types (Seq Scan vs Index Scan vs Bitmap Index Scan)
- MySQL: access_type (ALL vs ref vs range), key
- Mongo: scan_type (COLLSCAN vs IXSCAN vs TEXT_MATCH), docs_examined vs keys_examined
- Neo4j: total_db_hits, operators

---

## Wizualizacja wyników

Klasa `Visualizer` generuje wykresy z plików CSV/JSON:

| Wykres | Opis | Plik |
|--------|------|------|
| **CRUD grouped bar** | 4 subploty (INSERT/SELECT/UPDATE/DELETE), grouped bars per baza, z error barami (min/max) | `crud_{volume}_{label}.png` |
| **Detail per scenariusz** | Osobny wykres dla każdego scenariusza z porównaniem bez/z indeksami | `detail_{category}_{volume}.png` |
| **Index comparison** | 8 wariantów per scenariusz (4 bazy × 2 warianty indeksów) | `index_comparison_{volume}.png` |
| **Index heatmap** | Macierz ratio (z_idx / bez_idx) — kolory RdYlGn: <1.0 = indeks pomógł, >1.0 = spowolnił | `index_heatmap_{volume}.png` |
| **Volume scaling** | Linie — jak rośnie czas ze wzrostem wolumenu | `volume_scaling.png` |
| **Category summary** | Podsumowanie mediana per kategoria CRUD per wolumen | `category_summary.png` |
| **EXPLAIN summary** | Czas z EXPLAIN PostgreSQL per scenariusz SELECT (bez/z indeksami) | `explain_summary.png` |

Metryka: **mediana** (nie średnia) — odporna na outliers. Error bars: min–max.
Auto-log scale: jeśli rozrzut > 100×, oś Y automatycznie przechodzi na skalę logarytmiczną.

---

## Pipeline run-all — automatyzacja

Komenda `python main.py run-all --volume medium --trials 3` wykonuje **cały cykl** w jednym wywołaniu:

```
Step 1: generate --volume medium         (generowanie CSV)
Step 2: load --no-indexes --volume medium (ładowanie bez indeksów)
Step 3: benchmark --volume medium         (benchmarki bez indeksów)
Step 4: explain --volume medium           (EXPLAIN bez indeksów)
Step 5: load --volume medium              (przeładowanie Z indeksami)
Step 6: benchmark --volume medium --with-indexes (benchmarki z indeksami)
Step 7: explain --volume medium --with-indexes   (EXPLAIN z indeksami)
Step 8: visualize                         (generowanie wykresów)
```

To realizuje wymaganie regulaminu na 5.0: **"Automatyzacja testów oraz generowania wyników"**.

---

## Dane półustrukturalne (JSON) — element na 5.0

Tabela `content` posiada kolumnę `metadata`:
- **PostgreSQL**: `JSONB` + indeks GIN (`idx_content_metadata_gin`)
- **MySQL**: `JSON` + functional index na `metadata->>'$.studio'`
- **MongoDB**: natywny embedded document (bezpośrednio w dokumencie content)
- **Neo4j**: property map (przechowywany jako string, indeks TEXT)

Struktura metadata:
```json
{
  "studio": "Warner Bros.",
  "budget": 150000000,
  "awards": ["Oscar", "Golden Globe"],
  "tags": ["action", "sci-fi"],
  "production_countries": ["US", "UK"],
  "streaming_quality": {
    "max_resolution": "4K",
    "hdr_supported": true,
    "dolby_atmos": true
  }
}
```

Scenariusze wykorzystujące JSON:
- **S1** (Homepage) — odczyt `metadata->>'studio'` / `metadata.studio`
- **S4** (Full-text search) — odczyt `metadata->>'tags'` / `metadata.tags`

---

## Hipoteza badawcza

**H1: Indeksy a wydajność**
> Zastosowanie indeksów znacząco poprawia wydajność operacji SELECT kosztem spadku wydajności operacji INSERT i UPDATE.

Weryfikacja: porównanie wyników 24 scenariuszy przed i po zastosowaniu indeksów.
Narzędzie: heatmapa index ratio + wykresy index_comparison.

---

## Wolumeny danych

| Wolumen | Users | People | Content | Watch History | Łączna estymacja |
|---------|-------|--------|---------|---------------|------------------|
| **small** | 10K | 5K | 2K | 500K | ~600K rekordów |
| **medium** | 100K | 20K | 10K | 5M | ~6M rekordów |
| **large** | 1M | 50K | 30K | 50M | ~55M rekordów |

Obecnie benchmarki wykonane dla **small** i **medium**. Brakuje **large** (wymaga więcej czasu na uruchomienie).

---

## Schemat bazy — 12 tabel (relacyjne)

```
users (12 kolumn) ─┬─ profiles (8 kol.) ─┬─ watch_history (7 kol.)
                   │                      ├─ my_list (5 kol.)
                   │                      └─ ratings (7 kol.)
                   ├─ subscriptions (12 kol.) ── payments (9 kol.)
                   │
people (7 kol.) ──── content_people (5 kol.) ──── content (17 kol.)
                                                      ├─ seasons (5 kol.)
                                                      │     └─ episodes (8 kol.)
                                                      └─ metadata (JSONB/JSON)
```

Kaskadowe usuwanie (`ON DELETE CASCADE`) na wszystkich FK — to pozwala testować D1, D2, D5 jedną instrukcją w bazach relacyjnych.

---

## Infrastruktura Docker

4 kontenery z limitami pamięci (fair benchmark):
- **postgres:17** — 2 GB RAM, port 5432
- **mysql:8.0** — 2 GB RAM, port 3306
- **mongo:8.0** — 2 GB RAM, port 27017
- **neo4j:2026.02.2** — 4 GB RAM (heap 1G + transaction memory 1G), port 7687 + 7474, z pluginem APOC

---

## Sposób ładowania danych do baz

| Baza | Metoda | Dlaczego |
|------|--------|----------|
| **PostgreSQL** | `COPY FROM STDIN` (psycopg) | Natywny bulk-load, 10-100x szybszy niż INSERT |
| **MySQL** | Batch `INSERT ... VALUES (…),(…)` po 10K | `LOAD DATA INFILE` wymaga uprawnień FILE na serwerze, batch INSERT to najszybsza dostępna alternatywa |
| **MongoDB** | `insert_many(ordered=False)` w batchach po 10K | `ordered=False` pozwala na kontynuowanie po błędzie duplikatu |
| **Neo4j** | `UNWIND $rows AS r CREATE/MATCH` w batchach po 5K | Batch Cypher z parametrem listy — unika overhead pojedynczych transakcji |

---

## Aktualne wyniki (co mamy)

Pliki wynikowe:
- `results/benchmark_small_no_indexes.csv` (288 wierszy = 24 scenariusze × 4 bazy × 3 próby)
- `results/benchmark_small_with_indexes.csv`
- `results/benchmark_medium_no_indexes.csv`
- `results/benchmark_medium_with_indexes.csv`
- `results/explain/explain_{small,medium}_{no_indexes,with_indexes}.json`
- `results/charts/` — 18 wykresów PNG

---

## Checklist regulaminowy — pokrycie wymagań

### Ocena 3.0
- [x] 4 SZBD (2 relacyjne + 2 nierelacyjne)
- [x] Min. 5 tabel → mamy **12 tabel**
- [x] 12 scenariuszy testowych (min. 3 per CRUD) → mamy **24**
- [x] 3 próby per scenariusz
- [x] 3 wolumeny danych (small/medium/large)
- [x] Wizualizacja wyników

### Ocena 4.0
- [x] Min. 2 modele danych: **relacyjny** (PG, MySQL) + **dokumentowy** (MongoDB) + **grafowy** (Neo4j)
- [x] Wykorzystanie indeksów
- [x] Min. 10 tabel → mamy **12**
- [x] EXPLAIN analysis
- [x] Porównanie przed i po indeksach
- [x] 24 scenariusze (min. 6 per CRUD)
- [x] Wolumeny: 500K / 5M / 50M

### Ocena 5.0
- [x] Element 1: **Automatyzacja testów** — pipeline `run-all`
- [x] Element 2: **Dane półustrukturalne (JSON)** — kolumna metadata JSONB/JSON
- [x] Hipoteza badawcza **H1** — indeksy a wydajność

---

## Co jeszcze do zrobienia

1. **Uruchomienie benchmarków na wolumenie large** (1M users, 50M watch_history)
2. **Sprawozdanie pisemne** — opis teoretyczny SZBD, analiza wyników, weryfikacja H1
3. **Prezentacja** — wykresy + kluczowe wnioski
4. Opcjonalnie: rozszerzona analiza JSON (dodatkowe scenariusze filtrujące po polach metadata)
