# Opis projektu — flow uruchomienia

Projekt porownuje wydajnosc PostgreSQL, MySQL, MongoDB i Neo4j na modelu danych platformy streamingowej VOD. Caly pipeline uruchamia sie jednym poleceniem `python main.py run-all --volume small --trials 3` i przechodzi przez 8 krokow automatycznie.

---

## 1. Generacja danych

Pierwszy krok to wygenerowanie danych testowych do plikow CSV. Klasa `DataGenerator` generuje 12 plikow CSV — po jednym na tabele. Kolejnosc generacji ma znaczenie, bo pozniejsze tabele korzystaja z ID wygenerowanych wczesniej — np. `profiles` potrzebuje `user_id` z `users`, `episodes` potrzebuje `season_id` z `seasons`.

Dane generowane sa biblioteka Faker z ustalonym seedem 42, co daje pelna powtarzalnosc — kazde uruchomienie produkuje identyczne dane. Faker uzywa polskiego i angielskiego locale, wiec imiona, nazwiska i dane wygladaja realistycznie.

Rozmiary danych kontrolowane sa przez wolumeny zdefiniowane w `config.py`. Dla wolumenu small to 10 000 uzytkownikow, 5 000 osob (aktorzy/rezyserzy), 2 000 tresci i 500 000 wpisow historii ogladania. Medium to 10x wiecej, large 100x wiecej.

Wyjsciem sa czyste pliki CSV, ktore sa wspolnym zrodlem danych dla wszystkich czterech baz — kazda baza dostaje dokladnie te same dane.

---

## 2. Ladowanie danych bez indeksow

Krok drugi to zaladowanie CSV-ow do wszystkich czterech baz, ale celowo bez tworzenia indeksow wydajnosciowych. Kazda baza ma swoj loader z optymalizacjami pod bulk import:

**PostgreSQL** uzywa `COPY FROM STDIN` — natywnego mechanizmu bulk-load, ktory jest 10-100x szybszy niz zwykle INSERT-y. Dane sa streamowane binarnie z pliku CSV.

**MySQL** uzywa batch INSERT-ow po 5 000 wierszy naraz. Przed ladowaniem wylaczane sa `FOREIGN_KEY_CHECKS` i `UNIQUE_CHECKS` zeby przyspieszyc import — po zakonczeniu sa z powrotem wlaczane.

**MongoDB** laduje dane w modelu dokumentowym — to znaczy, ze schemat jest inny niz w bazach relacyjnych. Uzytkownicy maja zagniezdzone profile i aktywna subskrypcje jako subdokumenty wewnatrz jednego dokumentu. Tresci (content) maja zagniezdzone tablice cast (aktorzy/rezyserzy) i seasons z episodes. Uzywa `insert_many` w batchach po 5 000 z `ordered=False`.

**Neo4j** najpierw tworzy wezly (User, Profile, Content, Genre, Person, Season, Episode, Subscription, Payment), a potem relacje miedzy nimi (HAS_PROFILE, WATCHED, RATED, HAS_GENRE, ACTED_IN, DIRECTED, WROTE itd.). Ladowanie idzie batchami po 5 000 z uzyciem `UNWIND` w Cypher.

W trybie bez indeksow:
- PostgreSQL i MySQL — po zaladowaniu usuwane sa indeksy wydajnosciowe. Pozostaja jedynie klucze glowne i obce wynikajace ze schematu DDL.
- MongoDB — pomijane jest tworzenie indeksow, zostaje tylko domyslny `_id`.
- Neo4j — pomijane sa dodatkowe indeksy, zostaja jedynie constrainty unikalnosci na kluczach glownych (np. `User.user_id IS UNIQUE`), bo bez nich ladowanie relacji przez MATCH nie dziala.

---

## 3. Benchmarki bez indeksow

`BenchmarkRunner` wykonuje 24 scenariusze testowe — po 6 na kazda kategorie CRUD — na kazdej z 4 baz. Kazdy scenariusz to klasa dziedziczaca po `BaseScenario` z trzema fazami: setup, run, teardown. Mierzony jest wylacznie czas fazy `run` — za pomoca `time.perf_counter_ns()`, najdokladniejszego timera w Pythonie.

Faza **setup** przygotowuje dane potrzebne do testu — np. wstawia tymczasowy rekord ktory bedzie usuwany w scenariuszu DELETE, albo losuje istniejace ID do scenariusza SELECT. Faza **teardown** przywraca stan bazy do stanu sprzed testu — np. usuwa wstawione rekordy albo przywraca oryginalne wartosci pol. Dzieki temu kolejne scenariusze dzialaja na tych samych danych.

Dla kazdego scenariusza i kazdej bazy runner tworzy **nowe polaczenie** (nowa sesja TCP), co izoluje stan sesji. Przed pierwszym trialem czyszczone sa cache silnika bazodanowego: PostgreSQL dostaje `DISCARD ALL`, MySQL `FLUSH TABLES`, MongoDB `planCacheClear` na kazdej kolekcji, Neo4j `CALL db.clearQueryCaches()`. Cache jest czyszczony raz per scenariusz per baze — nie pomiedzy trialami. Dzieki temu pierwszy trial jest cold startem, a kolejne dwa korzystaja z rozgrzanego cache — mozna zaobserwowac wplyw cachowania na wydajnosc.

Warm-upy zostaly celowo wyrzucone. Nie ma wstepnych zapytan rozgrzewajacych — pierwszy trial scenariusza jest faktycznie cold startem po wyczyszczeniu cache.

Scenariusze obalaja pelne spektrum operacji:

**INSERT** — od pojedynczej rejestracji uzytkownika (wstawienie do 3 tabel), przez bulk import 10 000 rekordow historii ogladania (PostgreSQL uzywa COPY, MySQL batch INSERT, MongoDB insert_many, Neo4j UNWIND+CREATE), po dodanie serialu z pelnym drzewem (content + sezony + odcinki + aktorzy).

**SELECT** — od prostego filtrowania strony glownej z odczytem pola JSON (`metadata->>'studio'`), przez collaborative filtering (znajdz uzytkownikow o podobnych gustach i polec tresci), TOP 100 ogladalnosci z JOINem i GROUP BY, full-text search po tytule, az po filmografie osoby. Scenariusz S2 (rekomendacje) jest szczegolnie interesujacy bo Neo4j naturalnie modeluje przechodzenie po grafie, podczas gdy bazy relacyjne potrzebuja zlozonych self-JOINow na watch_history.

**UPDATE** — od pojedynczej aktualizacji postepu ogladania, przez przeliczenie sredniej oceny (subquery + update), masowa zmiane planu subskrypcji, az po masowa aktualizacje popularity_score dla wszystkich tresci wedlug formuly z views, avg_rating i liczby ocen.

**DELETE** — od kaskadowego usuwania tresci (PostgreSQL/MySQL usuwaja automatycznie przez ON DELETE CASCADE, MongoDB musi recznie usuwac z kazdej kolekcji osobno, Neo4j uzywa DETACH DELETE), przez czyszczenie starej historii ogladania (masowy DELETE po dacie), po masowe usuwanie nieaktywnych uzytkownikow.

Wyniki zapisywane sa do CSV — np. `benchmark_small_no_indexes.csv` — z kolumnami: scenario_id, scenario_name, category, database, volume, trial, time_ms, with_indexes. Kazdy scenariusz jest wykonywany 3 razy, zapisywany jest czas kazdego triala osobno.

---

## 4. EXPLAIN — analiza planow wykonania zapytan

### Czym jest EXPLAIN?

EXPLAIN to mechanizm diagnostyczny dostepny w kazdej bazie danych, ktory pozwala zajrzec "pod maske" i zobaczyc **jak baza danych wykonuje zapytanie**. Zamiast po prostu zwrocic wyniki, baza pokazuje swoj plan dzialania — jakiej strategii uzywa do znalezienia danych, ile rekordow musi przejrzec, czy korzysta z indeksow, ile czasu zajmuje kazdy krok.

### Po co nam EXPLAIN w projekcie?

Sam czas wykonania zapytania (mierzony w benchmarkach) mowi nam **ile** trwalo zapytanie, ale nie mowi **dlaczego** trwalo tyle. EXPLAIN daje odpowiedz na to drugie pytanie:

- Pozwala zweryfikowac czy indeksy sa faktycznie uzywane przez optymalizator — moze sie zdarzyc, ze tworzymy indeks, ale baza go ignoruje bo uzna ze full scan bedzie szybszy (np. na malej tabeli).
- Umozliwia porownanie strategii przed i po dodaniu indeksow — np. zmiana z Seq Scan na Index Scan w PostgreSQL, albo z COLLSCAN na IXSCAN w MongoDB.
- Pokazuje ile rekordow baza musiala przejrzec zeby znalezc wynik — jesli zapytanie zwraca 10 rekordow ale przejrzalo 500 000, to wiemy ze jest pole do optymalizacji.

W wizualizacji EXPLAIN summary wykres slupkowy pokazuje czas wykonania zapytan SELECT z i bez indeksow obok siebie. Dzieki temu na jednym wykresie widac nie tylko ze "jest szybciej", ale mozna zestawic to z informacja z planu (np. Seq Scan zamieniony na Index Scan) i wytlumaczyc przyczyne przyspieszenia.

### Jak dziala EXPLAIN w kazdej bazie

**PostgreSQL** — `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` — zwraca pelny plan z czasem planowania i wykonania, kosztami, typami skanow (Seq Scan, Index Scan, Bitmap Scan) i uzycie buforow. ANALYZE oznacza ze zapytanie jest faktycznie wykonywane, nie tylko estymowane.

**MySQL** — `EXPLAIN FORMAT=JSON` — zwraca plan z typem dostepu per tabela (ALL = full scan, ref = uzycie klucza, range = zakres), uzytymi kluczami i szacowana liczba przegladanych wierszy.

**MongoDB** — `.explain("executionStats")` — zwraca typ skanu (COLLSCAN = full scan, IXSCAN = uzycie indeksu, TEXT_MATCH = wyszukiwanie pelnotekstowe), liczbe przejrzanych dokumentow i kluczy indeksowych.

**Neo4j** — `PROFILE` jako prefix zapytania Cypher — zwraca drzewo operatorow z db_hits (ile razy silnik siegal do storage) i liczba przetworzonych wierszy.

Z kazdego planu wyciagane jest podsumowanie z kluczowymi metrykami. Wyniki zapisywane sa do JSON — np. `explain_small_no_indexes.json`.

---

## 5. Przeladowanie z indeksami

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

**MongoDB** dostaje 14 indeksow: unikalne na email i parach (profile_id, content_id), single field na polach filtrowanych, text index na title, compound index na (profile_id, started_at).

**Neo4j** dostaje 9 indeksow: property indexes na statusach i typach, text indexes na title i metadata.

---

## 6. Benchmarki z indeksami

Te same 24 scenariusze, ten sam kod, te same zapytania — jedyna roznica to obecnosc indeksow w bazach. Optymalizator kazdej bazy moze teraz wybrac Index Scan zamiast Seq Scan, uzyc indeksu do filtrowania lub sortowania. Wyniki zapisywane sa z flaga `with_indexes=True` do osobnego CSV.

---

## 7. EXPLAIN z indeksami

Identycznie jak krok 4, ale na bazach z indeksami. Kluczowe jest porownanie: czy optymalizator zmienil plan (np. Seq Scan -> Index Scan w PostgreSQL, COLLSCAN -> IXSCAN w MongoDB), czy zmniejszyla sie liczba przejrzanych wierszy, jak zmienil sie czas wykonania.

---

## 8. Wizualizacja

Ostatni krok generuje wykresy z uzyciem matplotlib, pandas i numpy. Klasa `Visualizer` wczytuje wszystkie pliki CSV z wynikami benchmarkow i pliki JSON z EXPLAIN, a nastepnie generuje 7 typow wykresow:

**CRUD grouped bar** — 4 subploty (INSERT/SELECT/UPDATE/DELETE), na kazdym slupki pogrupowane per scenariusz i per baza danych. Wartoscia jest mediana czasu z 3 triali, error bars pokazuja min i max. Jesli rozrzut przekracza 100x, os Y automatycznie przechodzi na skale logarytmiczna.

**Detail per scenariusz** — osobny subplot per scenariusz wewnatrz kategorii, z podzialem na "bez indeksow" i "z indeksami". Pozwala zobaczyc wplyw indeksow per konkretny scenariusz per baze.

**Index comparison** — 8 slupkow per scenariusz (4 bazy x 2 warianty indeksow), bezposrednie porownanie before/after. Wariant bez indeksow ma kolor pelny, z indeksami ma pattern ukosnych kresek.

**Index heatmap** — macierz scenariusze x bazy danych, wartosc to ratio czas_z_indeksami/czas_bez_indeksow. Kolorowanie RdYlGn_r: zielony oznacza ze indeks pomogl (<1.0), czerwony ze spowolnil (>1.0). Mozna natychmiast zobaczyc ktore scenariusze i ktore bazy najbardziej skorzystaly na indeksach.

**Volume scaling** — generowany tylko gdy sa wyniki dla wiecej niz jednego wolumenu. Linie per baza danych, os X to wolumeny, os Y to mediana czasu. Pokazuje jak rosnie czas ze wzrostem danych.

**Category summary** — podsumowanie mediany czasu per kategoria CRUD per baze danych, jeden subplot per wolumen.

**EXPLAIN summary** — wykres slupkowy czasu z EXPLAIN PostgreSQL per scenariusz SELECT, porownanie bez i z indeksami.

---

## Dane polustrukturalne (JSON)

### Czym sa dane polustrukturalne?

W klasycznej bazie relacyjnej kazda tabela ma sztywny schemat — z gory ustalone kolumny z okreslonymi typami. Jesli chcemy dodac nowe pole, musimy zrobic migracje (ALTER TABLE), co zmienia schemat dla wszystkich istniejacych rekordow.

Dane polustrukturalne to dane ktore maja pewna ogolna strukture, ale jej szczegoly moga sie roznic miedzy rekordami. W naszym projekcie przykladem jest kolumna `metadata` w tabeli `content`. Kazdy film czy serial moze miec inne metadane — jeden ma informacje o nagrodach Oscar, inny o budzecie, jeszcze inny o krajach koprodukcji. Zamiast tworzyc osobna kolumne na kazda mozliwa informacje (co daloby tabele z setkami kolumn, wiekszoscia pustych), przechowujemy to wszystko w jednym polu JSON.

### Jak to wyglada w praktyce?

Przykladowa wartosc `metadata` dla filmu:
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

### Po co to w naszym projekcie?

Kolumna `metadata` sluzy do zbadania jak kazda baza radzi sobie z danymi, ktore nie pasuja do sztywnego schematu tabelarycznego. To czesty problem w realnych aplikacjach — np. sklep internetowy gdzie rozne kategorie produktow maja rozne atrybuty (buty maja rozmiar, laptopy maja RAM, ksiazki maja ISBN).

Kazda baza podchodzi do tego inaczej:

**PostgreSQL** — przechowuje JSON jako typ JSONB (binarny JSON). Mozna odpytywac po dowolnych polach (`metadata->>'studio'`), a indeks GIN pozwala na szybkie przeszukiwanie calej struktury bez znajomosci jej ksztaltu.

**MySQL** — przechowuje JSON jako natywny typ. Mozna odczytywac pola operatorem `->>'$.studio'`, ale indeksowanie wymaga jawnego functional indexu na konkretnej sciezce — trzeba z gory wiedziec ktore pole chcemy indeksowac.

**MongoDB** — JSON to naturalny format przechowywania danych. Metadata jest po prostu czescia dokumentu, nie wymaga zadnego specjalnego traktowania. Mozna indeksowac dowolne zagniezdzone sciezki.

**Neo4j** — nie ma natywnego wsparcia dla zagniezdzonego JSON w propertiesach wezlow. Metadata przechowywana jest jako string i przeszukiwana text indexem.

Scenariusze S1 (strona glowna) i S4 (full-text search) odczytuja pola z metadata w zapytaniach — odpowiednio studio i tags. Dzieki temu mozna porownac jak szybko kazda baza radzi sobie z odpytywaniem danych polustrukturalnych.
