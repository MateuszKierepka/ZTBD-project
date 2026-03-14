# Plan realizacji projektu ZTBD — Platforma VOD

**Cel:** Ocena 5.0

**Technologie:** PostgreSQL | MySQL | MongoDB | Neo4j, Python 3.12+

---

## Struktura projektu

```
project/
├── docker-compose.yml          # 4 bazy danych
├── docker/
│   ├── postgres/init.sql       # schemat + indeksy PostgreSQL
│   ├── mysql/init.sql          # schemat + indeksy MySQL
│   └── neo4j/                  # konfiguracja
├── src/
│   ├── config.py               # connection strings, stale
│   ├── generators/
│   │   └── data_generator.py   # Faker-based generator
│   ├── loaders/
│   │   ├── postgres_loader.py
│   │   ├── mysql_loader.py
│   │   ├── mongo_loader.py
│   │   └── neo4j_loader.py
│   ├── benchmarks/
│   │   ├── runner.py           # orkiestracja testow
│   │   ├── scenarios.py        # 24 scenariusze CRUD
│   │   └── explain_analyzer.py # EXPLAIN ANALYZE
│   └── analysis/
│       ├── results_analyzer.py
│       └── visualizer.py       # wykresy matplotlib/plotly
├── results/                    # wyniki benchmarkow (CSV/JSON)
├── requirements.txt
└── README.md
```

---

## Fazy realizacji

### Faza 0 — Infrastruktura Docker

- `docker-compose.yml` z 4 serwisami:
  - `postgres:17` — port 5432
  - `mysql:9.2` — port 3306
  - `mongo:8` — port 27017
  - `neo4j:5` — port 7687 (Bolt) + 7474 (HTTP)
- Wolumeny Docker dla persystencji danych
- Ograniczenia pamieci per kontener (fair benchmarki)

### Faza 1 — Schematy baz danych

- **PostgreSQL** — DDL 12 tabel wg schematu v3 (FK, CHECK, UNIQUE), BEZ indeksow (dodamy osobno)
- **MySQL** — analogiczny DDL (InnoDB), roznice skladniowe (BIGSERIAL -> BIGINT AUTO_INCREMENT)
- **MongoDB** — 5 kolekcji (users, content, watch_history, ratings, payments) + JSON Schema validation
- **Neo4j** — constraints (unique) na node labels, relacje wg sekcji 9 schematu

### Faza 2 — Generowanie danych

- Generator Python + Faker (locale `pl_PL` + `en_US`)
- Dane do plikow CSV/JSON — raz wygenerowane, ladowane do 4 baz (te same dane = fair comparison)
- 3 wolumeny (zgodne z wymaganiami regulaminu: 500K / 1M / 10M rekordow):
  - Small: 10K users -> ~500K watch_history
  - Medium: 100K users -> ~5M watch_history
  - Large: 1M users -> ~50M watch_history
- Kolejnosc generowania (respektuje FK):
  ```
  users -> profiles -> subscriptions -> payments
  people -> content -> content_people -> seasons -> episodes
  watch_history, my_list, ratings
  ```

### Faza 3 — Ladowanie danych do baz

- **PostgreSQL**: `COPY FROM` (bulk insert)
- **MySQL**: `LOAD DATA INFILE` (bulk insert)
- **MongoDB**: `insert_many()` z batchami, transformacja do struktury dokumentowej (embedding)
- **Neo4j**: `UNWIND` + `CREATE` w transakcjach batchowych lub `neo4j-admin import`

### Faza 4 — Benchmarki CRUD (24 scenariusze)

#### INSERT (6 scenariuszy)

- I1. Rejestracja uzytkownika (multi-table: users + profiles + subscriptions)
- I2. Masowy import watch_history (batch 100K-10M rekordow)
- I3. Dodanie serialu z pelnym drzewem (content + seasons + episodes + content_people)
- I4. Batch insert platnosci (10K-100K rekordow do payments)
- I5. Dodanie ocen z przeliczeniem avg_rating (INSERT ratings + UPDATE content)
- I6. Import osob z powiazaniami (batch INSERT people + content_people)

#### SELECT (6 scenariuszy)

- S1. Strona glowna (filtrowanie + sortowanie po popularity_score, genres LIKE)
- S2. Rekomendacje collaborative filtering (zlozone JOIN-y watch_history)
- S3. TOP 100 tresci wg ogladalnosci w ostatnim miesiacu (agregacja)
- S4. Wyszukiwanie pelnotekstowe po tytule (LIKE/ILIKE vs text index)
- S5. Historia ogladania profilu z JOIN na content (50 ostatnich wpisow z tytulami)
- S6. Filmografia osoby (JOIN people -> content_people -> content)

#### UPDATE (6 scenariuszy)

- U1. Aktualizacja postepu ogladania (UPDATE watch_history.progress_percent)
- U2. Przeliczenie avg_rating (UPDATE content z podzapytaniem AVG z ratings)
- U3. Masowa zmiana planu subskrypcji (UPDATE z podzapytaniem na users)
- U4. Aktualizacja danych uzytkownika (UPDATE users SET email, phone)
- U5. Oznaczenie tresci jako nieaktywnej (UPDATE content.is_active)
- U6. Masowa aktualizacja popularity_score (UPDATE z formula obliczeniowa)

#### DELETE (6 scenariuszy)

- D1. Usuniecie tresci z kaskada (CASCADE: seasons, episodes, content_people, watch_history, my_list, ratings)
- D2. Usuniecie profilu z historia (CASCADE: watch_history, my_list, ratings)
- D3. Czyszczenie starej historii (DELETE watch_history WHERE started_at < rok temu)
- D4. Usuniecie z my_list (DELETE WHERE profile_id AND content_id)
- D5. Usuniecie subskrypcji z platnosciami (kaskadowe)
- D6. Masowe usuniecie nieaktywnych uzytkownikow (DELETE users WHERE status='deleted')

#### Metodologia testow

- Kazdy scenariusz x 4 bazy x 3 wolumeny x 3 proby = pomiar
- Mierzenie czasu: `time.perf_counter_ns()` (nanosekundowa precyzja)
- Wyniki do CSV: `scenario, database, volume, trial, time_ms`
- Testy BEZ indeksow -> dodanie indeksow -> te same testy Z indeksami
- **Indeksy na WSZYSTKICH 4 bazach:**
  - PostgreSQL: B-tree, GIN (full-text, jsonb), partial indexes
  - MySQL: B-tree, FULLTEXT, functional indexes
  - MongoDB: compound indexes, text indexes, partial indexes
  - Neo4j: property indexes (RANGE, TEXT, POINT), composite indexes

### Faza 5 — Analiza EXPLAIN

- PostgreSQL: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
- MySQL: `EXPLAIN ANALYZE`
- MongoDB: `.explain("executionStats")`
- Neo4j: `PROFILE` prefix do zapytan Cypher
- Porownanie planow przed i po indeksach

### Faza 6 — Wizualizacja wynikow

- Wykresy (matplotlib/plotly): grouped bar charts per scenariusz, linie per wolumen
- Tabele porownawcze: czas CRUD per baza per wolumen
- Heatmapy: impact indeksow (before/after ratio)
- Wnioski z EXPLAIN
- Porownanie wydajnosci operacji na danych JSON (element 5.0)

---

## Elementy zaawansowane (wymagane do oceny 5.0)

Wymagane min. 2 z 4 ponizszych + hipoteza badawcza.

**Wybrane 2 elementy:**

### Element 1: Automatyzacja testow oraz generowania wynikow
- `runner.py` — pelna orkiestracja: generowanie danych -> ladowanie -> benchmarki -> zbieranie wynikow
- Jedno wywolanie uruchamia caly pipeline dla wybranego wolumenu
- Wyniki automatycznie zapisywane do CSV/JSON w `results/`
- Raporty i wykresy generowane programowo (bez recznej interwencji)

### Element 2: Dane polustrukturalne (JSON)
- Dodanie kolumny `metadata JSONB` do tabeli `content` w PostgreSQL
- Dodanie kolumny `metadata JSON` do tabeli `content` w MySQL
- MongoDB — naturalnie przechowuje JSON (embedded documents w kolekcji `content`)
- Neo4j — przechowywanie jako property map na wezlach `Content`
- Przykladowe dane w `metadata`: `{"studio": "...", "budget": ..., "awards": [...], "tags": [...]}`
- Scenariusze testowe S1 i S4 beda wykorzystywac zapytania po polach JSON (filtrowanie, wyszukiwanie)
- W sprawozdaniu: porownanie wydajnosci operacji na danych JSON miedzy 4 silnikami

**Hipoteza badawcza:**
H1: Zastosowanie indeksow znaczaco poprawia wydajnosc operacji SELECT kosztem spadku wydajnosci operacji INSERT i UPDATE.
- Weryfikacja na podstawie danych z Fazy 4 (24 scenariusze x before/after indexes)
- Formalna prezentacja w sprawozdaniu: hipoteza zerowa, alternatywna, analiza wynikow

---

## Faza 7 — Sprawozdanie i prezentacja

### Sprawozdanie pisemne (wymagane przez regulamin)
- Cel i zakres pracy
- Opis wybranych SZBD (PostgreSQL, MySQL, MongoDB, Neo4j)
- Zalety i wady kazdego SZBD, udogodnienia i ograniczenia
- Czesc teoretyczna: awaryjnosc, bezpieczenstwo, migracje, integracje, skalowalnosc
- Obszary biznesowych zastosowan wybranych SZBD
- Opis zbioru danych (12 tabel, modele: relacyjny, dokumentowy, grafowy)
- Opis aplikacji testowej (wymagania, technologie, dzialanie)
- Opis testow wydajnosciowych + porownanie CRUD dla 3 wolumenow
- Analiza planow zapytan (EXPLAIN) — przed i po indeksach
- Rozszerzona analiza wynikow i wnioskow
- Weryfikacja hipotezy badawczej H1
- Wizualizacje (wykresy, tabele, heatmapy)

### Prezentacja
- Streszczenie kluczowych wynikow
- Wykresy porownawcze
- Wnioski i weryfikacja hipotezy

---

## Wybor technologii — uzasadnienie

**Python 3.12+** — najlepszy wybor do tego typu projektu:
- `Faker` (pl_PL) — realistyczne dane testowe
- `psycopg` / `pymysql` / `pymongo` / `neo4j` — natywne drivery do 4 baz
- `pandas` + `matplotlib` / `plotly` — analiza i wizualizacja wynikow
- Najprostszy do zadan benchmarkowych, najlepszy ekosystem data-tooling

**Docker Compose** — przenoszalnosc projektu, identyczne srodowisko na kazdej maszynie.
