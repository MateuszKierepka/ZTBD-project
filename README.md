# Projekt ZTBD — Porownanie wydajnosci 4 SZBD na modelu platformy VOD

**Technologie:** PostgreSQL | MySQL | MongoDB | Neo4j, Python 3.12+

Projekt porownuje wydajnosc PostgreSQL, MySQL, MongoDB i Neo4j na modelu danych platformy streamingowej VOD. Caly pipeline uruchamia sie jednym poleceniem `python main.py run-all --volume small --trials 3` i przechodzi przez 8 krokow automatycznie.

---

## Struktura projektu

```
app/
├── docker-compose.yml              # 4 bazy danych
├── docker/
│   ├── postgres/init.sql           # schemat PostgreSQL (DDL, bez indeksow)
│   └── mysql/init.sql              # schemat MySQL (DDL, bez indeksow)
├── main.py                         # CLI entry point (generate/load/benchmark/explain/visualize/run-all)
├── src/
│   ├── config.py                   # wolumeny, parametry generowania
│   ├── generators/
│   │   └── data_generator.py       # Faker-based generator (12 tabel CSV)
│   ├── loaders/
│   │   ├── postgres_loader.py      # COPY FROM + create/drop indexes
│   │   ├── mysql_loader.py         # batch INSERT + create/drop indexes
│   │   ├── mongo_loader.py         # insert_many + create indexes
│   │   └── neo4j_loader.py         # UNWIND + CREATE + create indexes
│   ├── benchmarks/
│   │   ├── base.py                 # BaseScenario, BenchmarkContext, VOLUME_PARAMS
│   │   ├── runner.py               # orkiestracja testow + flush cache
│   │   ├── insert_scenarios.py     # I1-I6
│   │   ├── select_scenarios.py     # S1-S6
│   │   ├── update_scenarios.py     # U1-U6
│   │   ├── delete_scenarios.py     # D1-D6
│   │   └── explain_analyzer.py     # EXPLAIN/PROFILE dla S1-S6
│   └── analysis/
│       └── visualizer.py           # wykresy matplotlib
├── data/                           # wygenerowane CSV (small/medium/large)
├── results/                        # wyniki benchmarkow (CSV/JSON)
│   ├── charts/                     # wygenerowane wykresy PNG
│   └── explain/                    # plany zapytan JSON
├── requirements.txt
└── README.md
```

---

## Infrastruktura Docker

4 kontenery z limitami pamieci (fair benchmark):
- **postgres:17** — 2 GB RAM, port 5432
- **mysql:8.0** — 2 GB RAM, port 3306
- **mongo:8.0** — 2 GB RAM, port 27017
- **neo4j:2026.02.2** — 4 GB RAM (heap 1G + transaction memory 1G), port 7687 + 7474, z pluginem APOC

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

Kaskadowe usuwanie (`ON DELETE CASCADE`) na wszystkich FK — to pozwala testowac D1, D2, D5 jedna instrukcja w bazach relacyjnych.

---

## Wolumeny danych

| Wolumen | Users | People | Content | Watch History | Laczna estymacja |
|---------|-------|--------|---------|---------------|------------------|
| **small** | 10K | 5K | 2K | 500K | ~600K rekordow |
| **medium** | 100K | 20K | 10K | 5M | ~6M rekordow |
| **large** | 1M | 50K | 30K | 50M | ~55M rekordow |

---

## Pipeline run-all — automatyzacja

Komenda `python main.py run-all --volume medium --trials 3` wykonuje **caly cykl** w jednym wywolaniu:

```
Step 1: generate --volume medium         (generowanie CSV)
Step 2: load --no-indexes --volume medium (ladowanie bez indeksow)
Step 3: benchmark --volume medium         (benchmarki bez indeksow)
Step 4: explain --volume medium           (EXPLAIN bez indeksow)
Step 5: load --volume medium              (przeladowanie Z indeksami)
Step 6: benchmark --volume medium --with-indexes (benchmarki z indeksami)
Step 7: explain --volume medium --with-indexes   (EXPLAIN z indeksami)
Step 8: visualize                         (generowanie wykresow)
```

---

## Krok 1 — Generacja danych

Klasa `DataGenerator` generuje 12 plikow CSV — po jednym na tabele. Kolejnosc generacji ma znaczenie, bo pozniejsze tabele korzystaja z ID wygenerowanych wczesniej — np. `profiles` potrzebuje `user_id` z `users`, `episodes` potrzebuje `season_id` z `seasons`.

Dane generowane sa biblioteka Faker z ustalonym seedem 42, co daje pelna powtarzalnosc — kazde uruchomienie produkuje identyczne dane. Faker uzywa polskiego i angielskiego locale, wiec imiona, nazwiska i dane wygladaja realistycznie.

Rozmiary danych kontrolowane sa przez wolumeny zdefiniowane w `config.py`. Wyjsciem sa czyste pliki CSV, ktore sa wspolnym zrodlem danych dla wszystkich czterech baz — kazda baza dostaje dokladnie te same dane.

---

## Krok 2 — Ladowanie danych do baz

Ladowanie CSV-ow do wszystkich czterech baz, celowo bez tworzenia indeksow wydajnosciowych. Kazda baza ma swoj loader z optymalizacjami pod bulk import:

| Baza | Metoda | Dlaczego |
|------|--------|----------|
| **PostgreSQL** | `COPY FROM STDIN` (psycopg) | Natywny bulk-load, 10-100x szybszy niz INSERT |
| **MySQL** | Batch `INSERT ... VALUES (…),(…)` po 5 000 | `LOAD DATA INFILE` wymaga uprawnien FILE na serwerze, batch INSERT to najszybsza dostepna alternatywa |
| **MongoDB** | `insert_many(ordered=False)` w batchach po 5 000 | `ordered=False` pozwala na kontynuowanie po bledzie duplikatu |
| **Neo4j** | `UNWIND $rows AS r CREATE/MATCH` w batchach po 5 000 | Batch Cypher z parametrem listy — unika overhead pojedynczych transakcji |

**MongoDB** laduje dane w modelu dokumentowym — schemat jest inny niz w bazach relacyjnych. Uzytkownicy maja zagniezdzone profile i aktywna subskrypcje jako subdokumenty wewnatrz jednego dokumentu. Tresci (content) maja zagniezdzone tablice cast (aktorzy/rezyserzy) i seasons z episodes.

**Neo4j** najpierw tworzy wezly (User, Profile, Content, Genre, Person, Season, Episode, Subscription, Payment), a potem relacje miedzy nimi (HAS_PROFILE, WATCHED, RATED, HAS_GENRE, ACTED_IN, DIRECTED, WROTE itd.).

W trybie bez indeksow:
- PostgreSQL i MySQL — po zaladowaniu usuwane sa indeksy wydajnosciowe. Pozostaja jedynie klucze glowne i obce wynikajace ze schematu DDL.
- MongoDB — pomijane jest tworzenie indeksow, zostaje tylko domyslny `_id`.
- Neo4j — pomijane sa dodatkowe indeksy, zostaja jedynie constrainty unikalnosci na kluczach glownych (np. `User.user_id IS UNIQUE`), bo bez nich ladowanie relacji przez MATCH nie dziala.

---

## Krok 3 — Benchmarki (24 scenariusze CRUD)

### Architektura benchmarkow

**Klasa bazowa (`base.py`)** — `BaseScenario` — kazdy scenariusz implementuje wzorzec **setup → run → teardown** per baza:
- `setup(db_type, conn, ctx)` — przygotowanie danych testowych (np. wstawienie rekordu do usuniecia)
- `run(db_type, conn, ctx)` — **wlasciwa operacja, ktora jest mierzona** (`time.perf_counter_ns()`)
- `teardown(db_type, conn, ctx)` — sprzatanie, przywrocenie stanu bazy

Kazdy scenariusz ma **4 implementacje** (run_postgres, run_mysql, run_mongo, run_neo4j). Gdy runner wywoluje `run("postgres", ...)`, klasa bazowa automatycznie wybiera odpowiednia metode `run_postgres()`.

**`BenchmarkContext`** — dostarcza:
- `max_ids` — wartosci MAX(id) per tabela (zeby nie kolidowac z istniejacymi danymi)
- `test_id(table)` — generuje unikalne ID dla danych testowych (max + 100_000 + counter)
- `random_id(table)` — losowy istniejacy ID (do SELECT/UPDATE)
- `params` — parametry zalezne od wolumenu (np. batch_watch_history: 10K/100K/500K)

### Runner (`runner.py`)

`BenchmarkRunner` — orkiestracja:
1. `connect(databases)` — otwiera polaczenia do wybranych baz
2. `build_context()` — pobiera MAX(id) z PostgreSQL dla wszystkich tabel
3. **Dla kazdego scenariusza x kazdej bazy:**
   - Tworzy **NOWE polaczenie** (nowa sesja TCP = czysty stan sesji)
   - `_flush_caches()` — czyszczenie cache na poziomie silnika
   - Petla 3 triali: `setup → run (mierzony) → teardown`
   - Zamkniecie polaczenia
4. `save_results()` — zapis do CSV: `scenario_id, scenario_name, category, database, volume, trial, time_ms, with_indexes`

### Cold start — brak warm-upow

Przed scenariuszem **NIE wykonujemy rozgrzewki**. Trial 1 zawsze startuje "na zimno":
- Trial 1 jest naturalnie wolniejszy (cold start) — cache silnika jest pusty, plany zapytan nie sa skompilowane
- Trialy 2 i 3 korzystaja z "rozgrzanego" cache (buffer pool, plan cache) — sa szybsze
- Pozwala to zaobserwowac **wplyw cache'owania silnika bazodanowego na wydajnosc**
- Przykladowy wzorzec: `S1 | postgres | avg=45.2ms (82.3, 30.1, 23.2)` — trial 1 wyraznie wolniejszy

### Czyszczenie cache miedzy scenariuszami

Przed kazdym scenariuszem `_flush_caches()` czysci cache na poziomie silnika, zeby kazdy scenariusz startowal z czystym stanem:

| Baza | Komenda | Co czysci |
|------|---------|-----------|
| **PostgreSQL** | `DISCARD ALL` | Prepared statements, parametry sesji, tymczasowe tabele |
| **MySQL** | `FLUSH TABLES` | Zamyka i wymusza ponowne otwarcie plikow tabel |
| **MongoDB** | `planCacheClear` na 5 kolekcjach | Cache planow zapytan (query plan cache) |
| **Neo4j** | `CALL db.clearQueryCaches()` | Cache planow zapytan Cypher |

**Ograniczenie:** Cache na poziomie buffer pool (PostgreSQL shared_buffers, InnoDB buffer pool, WiredTiger cache, Neo4j page cache) nie moze byc czyszczony bez restartu serwera. To ograniczenie systemowe — niezalezne od aplikacji benchmarkowej.

### Dlaczego takie podejscie?

- **Nowe polaczenie per scenariusz+baza** — izolacja sesji, brak wplywu stanu poprzedniego scenariusza
- **Flush caches** — czyszczenie cache planow zapytan, zeby kazdy scenariusz czynal od czystego stanu silnika
- **Brak warm-upow** — celowo zachowujemy efekt cold start na trialu 1 (widoczny wplyw cache)
- **Setup/teardown** — test nie zmienia stanu bazy, dane po benchmarku sa takie same jak przed
- **perf_counter_ns()** — najdokladniejszy timer w Pythonie (nanosekundowa precyzja)
- **Mierzone jest TYLKO `run()`** — setup i teardown nie wchodza w czas

---

## 24 scenariusze testowe — szczegoly

### INSERT (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **I1** | Rejestracja uzytkownika | Wstawienie danych do 3 tabel: users + profiles + subscriptions |
| **I2** | Bulk import watch_history | Masowe wstawienie N rekordow (10K–500K zalezenie od wolumenu) |
| **I3** | Dodanie serialu z pelnym drzewem | Wstawienie content + 3 sezony x 10 odcinkow + 10 aktorow |
| **I4** | Batch insert platnosci | Masowe wstawienie N rekordow do payments (1K–50K) |
| **I5** | Oceny z przeliczeniem avg_rating | Masowe wstawienie ocen + przeliczenie sredniej oceny na content |
| **I6** | Import osob z powiazaniami | Masowe wstawienie osob (people) + przypisanie ich do tresci (content_people) |

**Jak rozni sie implementacja miedzy bazami (INSERT):**
- **PG** — masowe operacje przez `COPY FROM STDIN` (natywny bulk-load PostgreSQL, najszybszy sposob), pojedyncze przez zwykle INSERT z `RETURNING user_id` zeby dostac ID nowego rekordu
- **MySQL** — masowe przez batch INSERT (wstawianie wielu wierszy jednym zapytaniem, po 5 000 naraz), ID nowego rekordu przez `lastrowid`
- **MongoDB** — masowe przez `insert_many()` (wrzuca liste dokumentow naraz), w I1 caly user to jeden dokument z zagniezdzonymi profilami i subskrypcja (nie ma osobnych tabel), w I6 zamiast osobnej tabeli content_people dodajemy aktorow do tablicy `cast` wewnatrz dokumentu content
- **Neo4j** — masowe przez `UNWIND` (przekazujemy liste danych jako parametr, baza iteruje po niej), tworzy wezly i relacje miedzy nimi, np. `(Profile)-[:WATCHED]->(Content)`

### SELECT (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **S1** | Strona glowna | Filtrowanie tresci po gatunku "Action", sortowanie po popularnosci, zwrocenie 20 najlepszych + odczyt pola JSON (studio) |
| **S2** | Collaborative filtering | Znajdz uzytkownikow o podobnych gustach (ogladali to samo), polecaj tresci ktorych nasz user jeszcze nie widzial |
| **S3** | TOP 100 ogladalnosci | Policz ile razy kazda tresc byla ogladana w ostatnim miesiacu, zwroc 100 najpopularniejszych |
| **S4** | Full-text search po tytule | Wyszukiwanie tresci po fragmencie tytulu + odczyt pola JSON (tags) |
| **S5** | Historia ogladania profilu | 50 ostatnich pozycji z historii ogladania danego profilu, razem z tytulami tresci |
| **S6** | Filmografia osoby | Wszystkie tresci w ktorych dana osoba brala udzial (jako aktor/rezyser/scenarzysta) |

**Jak rozni sie implementacja miedzy bazami (SELECT):**
- **PG/MySQL** — klasyczne JOIN-y miedzy tabelami, WHERE do filtrowania, GROUP BY do agregacji. W S1 dostep do pola JSON przez `metadata->>'studio'` (PG) / `metadata->>'$.studio'` (MySQL). W S4 wyszukiwanie przez `ILIKE '%term%'` (PG) / `LIKE '%term%'` (MySQL)
- **MongoDB** — zamiast JOIN-ow uzywa `aggregate` pipeline (lancuch operacji: filtruj → grupuj → sortuj → dolacz dane z innej kolekcji przez `$lookup`). W S4 wyszukiwanie przez wbudowany text search (`$text`). W S6 wystarczy jedno zapytanie bo aktorzy sa zagniezdeni w dokumencie content
- **Neo4j** — zapytania Cypher oparte na przechodzeniu po grafie, np. "znajdz profil, przejdz po relacji WATCHED do tresci, przejdz dalej do innych profili ktore tez to ogladaly". Naturalnie modeluje rekomendacje (S2) — graf jest do tego stworzony

### UPDATE (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **U1** | Aktualizacja postepu | Zmiana procentu obejrzenia w historii ogladania |
| **U2** | Przeliczenie avg_rating | Obliczenie sredniej oceny ze wszystkich recenzji i zapisanie jej na tresci |
| **U3** | Masowa zmiana planu | Zmiana planu subskrypcji z "Basic" na "Standard" dla wszystkich aktywnych uzytkownikow |
| **U4** | Aktualizacja danych usera | Zmiana emaila i telefonu uzytkownika |
| **U5** | Oznaczenie tresci jako nieaktywnej | Ustawienie flagi is_active = false na wybranej tresci |
| **U6** | Masowa aktualizacja popularity_score | Przeliczenie wyniku popularnosci dla WSZYSTKICH tresci wg formuly (views x 0.0001 + avg_rating x 5 + liczba_ocen x 0.1) |

**Jak rozni sie implementacja miedzy bazami (UPDATE):**
- **PG/MySQL** — klasyczny UPDATE z WHERE, w U2 i U6 podzapytanie do obliczenia wartosci (np. `SET avg_rating = (SELECT AVG(score) FROM ratings WHERE ...)`)
- **MongoDB** — `update_one` / `update_many` na dokumentach. W U3 subskrypcja jest zagniezdzona w dokumencie usera, wiec aktualizujemy pole `subscription.plan_name` wewnatrz dokumentu. W U6 trzeba najpierw policzyc oceny osobnym zapytaniem, a potem zrobic masowa aktualizacje
- **Neo4j** — `MATCH` + `SET` na wezlach/relacjach. W U2 oblicza srednia z relacji `RATED` i ustawia ja na wezle Content

### DELETE (6 scenariuszy)

| ID | Nazwa | Co robi |
|----|-------|---------|
| **D1** | Usuniecie tresci z kaskada | Usuniecie tresci razem ze wszystkimi powiazanymi danymi (sezony, odcinki, historia, lista, oceny) |
| **D2** | Usuniecie profilu z historia | Usuniecie profilu razem z jego historia ogladania, lista i ocenami |
| **D3** | Czyszczenie starej historii | Usuniecie wszystkich wpisow historii ogladania starszych niz rok |
| **D4** | Usuniecie z my_list | Usuniecie jednej pozycji z listy "do obejrzenia" |
| **D5** | Usuniecie subskrypcji z platnosciami | Usuniecie subskrypcji razem z jej platnosciami |
| **D6** | Masowe usuniecie nieaktywnych userow | Usuniecie wszystkich uzytkownikow ze statusem "deleted" |

**Jak rozni sie implementacja miedzy bazami (DELETE):**
- **PG/MySQL** — kaskadowe usuwanie dziala automatycznie dzieki `ON DELETE CASCADE` na kluczach obcych — wystarczy usunac rodzica (np. content), a baza sama usunie sezony, odcinki, historie itp.
- **MongoDB** — nie ma CASCADE, trzeba recznie usuwac z kazdej kolekcji osobno (np. w D1: usun z watch_history, my_list, ratings, i dopiero potem z content). W D2 profil jest zagniezdony w userze, wiec usuwamy go operacja `$pull` (wyciagniecie elementu z tablicy)
- **Neo4j** — `DETACH DELETE` usuwa wezel razem ze wszystkimi jego relacjami. W D3 usuwanie batchowane (po 10 000 naraz w petli), bo usuniecie milionow relacji na raz mogloby spowodowac brak pamieci

### Wyniki benchmarkow

Wyniki zapisywane sa do CSV — np. `benchmark_small_no_indexes.csv` — z kolumnami: scenario_id, scenario_name, category, database, volume, trial, time_ms, with_indexes. Kazdy scenariusz jest wykonywany 3 razy, zapisywany jest czas kazdego triala osobno.

---

## Krok 4 — EXPLAIN — analiza planow wykonania zapytan

### Czym jest EXPLAIN?

EXPLAIN to mechanizm diagnostyczny dostepny w kazdej bazie danych, ktory pozwala zajrzec "pod maske" i zobaczyc **jak baza danych wykonuje zapytanie**. Zamiast po prostu zwrocic wyniki, baza pokazuje swoj plan dzialania — jakiej strategii uzywa do znalezienia danych, ile rekordow musi przejrzec, czy korzysta z indeksow, ile czasu zajmuje kazdy krok.

### Po co nam EXPLAIN w projekcie?

Sam czas wykonania zapytania (mierzony w benchmarkach) mowi nam **ile** trwalo zapytanie, ale nie mowi **dlaczego** trwalo tyle. EXPLAIN daje odpowiedz na to drugie pytanie:

- Pozwala zweryfikowac czy indeksy sa faktycznie uzywane przez optymalizator — moze sie zdarzyc, ze tworzymy indeks, ale baza go ignoruje bo uzna ze full scan bedzie szybszy (np. na malej tabeli).
- Umozliwia porownanie strategii przed i po dodaniu indeksow — np. zmiana z Seq Scan na Index Scan w PostgreSQL, albo z COLLSCAN na IXSCAN w MongoDB.
- Pokazuje ile rekordow baza musiala przejrzec zeby znalezc wynik — jesli zapytanie zwraca 10 rekordow ale przejrzalo 500 000, to wiemy ze jest pole do optymalizacji.

### Jak dziala EXPLAIN w kazdej bazie

| Baza | Metoda | Format |
|------|--------|--------|
| **PostgreSQL** | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` | JSON z Planning Time, Execution Time, scan types |
| **MySQL** | `EXPLAIN FORMAT=JSON` | JSON z access_type, key, rows_examined |
| **MongoDB** | `.explain("executionStats")` | nReturned, totalDocsExamined, totalKeysExamined, scan_type (COLLSCAN vs IXSCAN) |
| **Neo4j** | `PROFILE` prefix | db_hits, rows, operator types |

Z kazdego planu wyciagane jest podsumowanie z kluczowymi metrykami:
- PG: execution_time_ms, scan_types (Seq Scan vs Index Scan vs Bitmap Index Scan)
- MySQL: access_type (ALL vs ref vs range), key
- Mongo: scan_type (COLLSCAN vs IXSCAN vs TEXT_MATCH), docs_examined vs keys_examined
- Neo4j: total_db_hits, operators

Wyniki zapisywane do `results/explain/explain_{volume}_{label}.json`.

---

## Krok 5 — Przeladowanie z indeksami

Dane sa ladowane ponownie od zera — TRUNCATE w PostgreSQL/MySQL, drop_collection w MongoDB, usuwanie wszystkich wezlow i relacji w Neo4j — i dopiero potem tworzone sa indeksy wydajnosciowe. Przeladowanie od zera gwarantuje ze indeksy sa budowane na kompletnych danych.

### Typy indeksow uzywane w projekcie

**B-tree** — najpopularniejszy typ indeksu, uzywany domyslnie w PostgreSQL i MySQL. Przechowuje dane w zbalansowanym drzewie, ktore pozwala na szybkie wyszukiwanie, sortowanie i zakresowe zapytania (WHERE x > 100). Dziala dobrze na kolumnach z duza liczba unikalnych wartosci (np. email, user_id).

**Composite (zlozony)** — indeks na kilku kolumnach jednoczesnie, np. `(profile_id, started_at DESC)`. Kolejnosc kolumn ma znaczenie — indeks jest efektywny tylko gdy zapytanie filtruje po pierwszej kolumnie lub po obu. Filtrowanie tylko po drugiej kolumnie (np. samo `started_at`) nie skorzysta z tego indeksu.

**Unique** — indeks z dodatkowa gwarancja unikalnosci wartosci, np. na kolumnie `email`. Obok przyspieszenia wyszukiwania wymusza tez integralnosc danych — baza nie pozwoli wstawic duplikatu.

**GIN (Generalized Inverted Index)** — specjalny typ indeksu w PostgreSQL do indeksowania struktur zlozonych. Uzywany na kolumnie `metadata` (JSONB) do szybkiego przeszukiwania calej struktury JSON, oraz z rozszerzeniem `pg_trgm` na `title` do wyszukiwania ILIKE z wildcardami.

**FULLTEXT** — indeks do wyszukiwania pelnotekstowego (MySQL). Zamiast szukac dokladnego dopasowania, pozwala wyszukiwac slowa w tekscie z uwzglednieniem odmiany i relewantnosci.

**Text index** — odpowiednik FULLTEXT w MongoDB. Pozwala na wyszukiwanie pelnotekstowe za pomoca operatora `$text`.

**Partial index** — indeks ktory obejmuje tylko czesc danych spelniajaca warunek, np. `WHERE is_active = TRUE` w PostgreSQL. Mniejszy rozmiar indeksu i szybsze operacje, bo indeksowane sa tylko rekordy ktore faktycznie beda wyszukiwane.

**Functional index** — indeks na wyniku wyrazenia, np. w MySQL `CAST(metadata->>'$.studio' AS CHAR(100))`. Pozwala indeksowac wartosc wyciagnieta z JSON bez zmiany schematu tabeli.

### Ile indeksow dostaje kazda baza

**PostgreSQL** dostaje 17 indeksow: standardowe B-tree na kolumnach filtrowanych (status, user_id, content_id itd.), partial index na `popularity_score DESC WHERE is_active = TRUE`, GIN trigram na `title`, GIN na `metadata` (JSONB), composite `(profile_id, started_at DESC)` na watch_history.

**MySQL** dostaje 10 indeksow: B-tree, FULLTEXT na `title`, functional index na `CAST(metadata->>'$.studio' AS CHAR(100))`, composite `(profile_id, started_at DESC)`.

**MongoDB** dostaje 16 indeksow: unikalne na email i parach (profile_id, content_id), single field na polach filtrowanych, text index na title, compound index na (profile_id, started_at).

**Neo4j** dostaje 9 indeksow: property indexes (RANGE) na statusach i typach, text indexes na title i metadata.

---

## Krok 6 — Benchmarki z indeksami

Te same 24 scenariusze, ten sam kod, te same zapytania — jedyna roznica to obecnosc indeksow w bazach. Optymalizator kazdej bazy moze teraz wybrac Index Scan zamiast Seq Scan, uzyc indeksu do filtrowania lub sortowania. Wyniki zapisywane sa z flaga `with_indexes=True` do osobnego CSV.

---

## Krok 7 — EXPLAIN z indeksami

Identycznie jak krok 4, ale na bazach z indeksami. Kluczowe jest porownanie: czy optymalizator zmienil plan (np. Seq Scan -> Index Scan w PostgreSQL, COLLSCAN -> IXSCAN w MongoDB), czy zmniejszyla sie liczba przejrzanych wierszy, jak zmienil sie czas wykonania.

---

## Krok 8 — Wizualizacja

Ostatni krok generuje wykresy z uzyciem matplotlib, pandas i numpy. Klasa `Visualizer` wczytuje wszystkie pliki CSV z wynikami benchmarkow i pliki JSON z EXPLAIN, a nastepnie generuje 9 typow wykresow:

| Wykres | Opis | Plik |
|--------|------|------|
| **CRUD grouped bar** | 4 subploty (INSERT/SELECT/UPDATE/DELETE), grouped bars per baza, z error barami (min/max). Metryka: mediana czasu z 3 triali. | `crud_{volume}_{label}.png` |
| **Detail per scenariusz** | Osobny subplot per scenariusz wewnatrz kategorii, z podzialem na "bez indeksow" i "z indeksami". Pozwala zobaczyc wplyw indeksow per konkretny scenariusz per baze. | `detail_{category}_{volume}.png` |
| **Index comparison** | 8 slupkow per scenariusz (4 bazy x 2 warianty indeksow), bezposrednie porownanie before/after. Wariant bez indeksow ma kolor pelny, z indeksami ma pattern ukosnych kresek. | `index_comparison_{volume}.png` |
| **Index heatmap** | Macierz scenariusze x bazy danych, wartosc to ratio czas_z_indeksami/czas_bez_indeksow. Kolorowanie RdYlGn_r: zielony oznacza ze indeks pomogl (<1.0), czerwony ze spowolnil (>1.0). | `index_heatmap_{volume}.png` |
| **Volume scaling** | Linie per baza danych, os X to wolumeny, os Y to mediana czasu. Pokazuje jak rosnie czas ze wzrostem danych. Generowany tylko gdy sa wyniki dla wiecej niz jednego wolumenu. | `volume_scaling.png` |
| **Category summary** | Podsumowanie mediany czasu per kategoria CRUD per baze danych, jeden subplot per wolumen. | `category_summary.png` |
| **EXPLAIN scan changes** | Macierz scenariusze SELECT x bazy danych pokazujaca zmiane typu skanu po dodaniu indeksow (np. Seq Scan -> Index Scan). Kolor zielony = zmiana typu skanu (indeks pomaga), zolty = brak zmiany. | `explain_scan_changes_{volume}.png` |
| **EXPLAIN rows examined** | Wykres slupkowy liczby przejrzanych wierszy/dokumentow/db_hits per scenariusz SELECT per baze, porownanie bez i z indeksami. Auto-log scale przy duzym rozrzucie. | `explain_rows_examined_{volume}.png` |
| **EXPLAIN exec time** | Wykres slupkowy czasu wykonania zapytan SELECT z EXPLAIN per scenariusz, porownanie bez i z indeksami. Dane z PostgreSQL i MongoDB (bazy ktore zwracaja czas wykonania w EXPLAIN). | `explain_exec_time_{volume}.png` |

Metryka: **mediana** (nie srednia) — odporna na outliers. Error bars: min–max.
Auto-log scale: jesli rozrzut > 100x, os Y automatycznie przechodzi na skale logarytmiczna.

---

## Dane polustrukturalne (JSON)

### Czym sa dane polustrukturalne?

W klasycznej bazie relacyjnej kazda tabela ma sztywny schemat — z gory ustalone kolumny z okreslonymi typami. Dane polustrukturalne to dane ktore maja pewna ogolna strukture, ale jej szczegoly moga sie roznic miedzy rekordami. W projekcie przykladem jest kolumna `metadata` w tabeli `content`. Kazdy film czy serial moze miec inne metadane — jeden ma informacje o nagrodach Oscar, inny o budzecie, jeszcze inny o krajach koprodukcji. Zamiast tworzyc osobna kolumne na kazda mozliwa informacje, przechowujemy to wszystko w jednym polu JSON.

### Przykladowa wartosc metadata

```json
{
  "studio": "Warner Bros",
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

### Jak kazda baza przechowuje JSON

| Baza | Typ | Indeks | Opis |
|------|-----|--------|------|
| **PostgreSQL** | `JSONB` | GIN (`idx_content_metadata_gin`) | Binarny JSON, mozna odpytywac po dowolnych polach (`metadata->>'studio'`), GIN pozwala na szybkie przeszukiwanie calej struktury |
| **MySQL** | `JSON` | Functional index na `metadata->>'$.studio'` | Natywny JSON, odczyt pol operatorem `->>'$.studio'`, indeksowanie wymaga jawnego functional indexu na konkretnej sciezce |
| **MongoDB** | Natywny embedded document | Standardowe indeksy na dowolnych polach | JSON to naturalny format przechowywania danych, metadata jest po prostu czescia dokumentu |
| **Neo4j** | String (property map) | TEXT index | Brak natywnego wsparcia dla zagniezdzonego JSON w propertiesach wezlow, przeszukiwanie text indexem |

Scenariusze wykorzystujace JSON:
- **S1** (Homepage) — odczyt `metadata->>'studio'` / `metadata.studio`
- **S4** (Full-text search) — odczyt `metadata->>'tags'` / `metadata.tags`

---

## Hipoteza badawcza

**H1: Indeksy a wydajnosc**
> Zastosowanie indeksow znaczaco poprawia wydajnosc operacji SELECT kosztem spadku wydajnosci operacji INSERT i UPDATE.

Weryfikacja: porownanie wynikow 24 scenariuszy przed i po zastosowaniu indeksow.
Narzedzie: heatmapa index ratio + wykresy index_comparison.

---

## Wybor technologii — uzasadnienie

**Python 3.12+** — najlepszy wybor do tego typu projektu:
- `Faker` (pl_PL) — realistyczne dane testowe
- `psycopg` / `pymysql` / `pymongo` / `neo4j` — natywne drivery do 4 baz
- `pandas` + `matplotlib` — analiza i wizualizacja wynikow

**Docker Compose** — przenosalnosc projektu, identyczne srodowisko na kazdej maszynie.

---

## Aktualne wyniki

Pliki wynikowe:
- `results/benchmark_small_no_indexes.csv` (288 wierszy = 24 scenariusze x 4 bazy x 3 proby)
- `results/benchmark_small_with_indexes.csv`
- `results/benchmark_medium_no_indexes.csv`
- `results/benchmark_medium_with_indexes.csv`
- `results/explain/explain_{small,medium}_{no_indexes,with_indexes}.json`
- `results/charts/` — 24 wykresy PNG

---

## Co jeszcze do zrobienia

1. **Uruchomienie benchmarkow na wolumenie large** (1M users, 50M watch_history)
2. **Sprawozdanie pisemne** — opis teoretyczny SZBD, analiza wynikow, weryfikacja H1
3. **Prezentacja** — wykresy + kluczowe wnioski
