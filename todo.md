Wizualizer (visualizer.py:406-508) wyciąga z plików explain:                                                                                                                                                    
  ┌────────────┬────────────────────────────────────┬────────────────────────────────────┐               
  │    Baza    │       Klucz użyty do wykresu       │     Klucz użyty do opisu scan      │
  ├────────────┼────────────────────────────────────┼────────────────────────────────────┤
  │ PostgreSQL │ summary.postgres.execution_time_ms │ summary.postgres.scan_types[].type │
  ├────────────┼────────────────────────────────────┼────────────────────────────────────┤
  │ MongoDB    │ summary.mongo.execution_time_ms    │ summary.mongo.scan_type            │
  ├────────────┼────────────────────────────────────┼────────────────────────────────────┤
  │ MySQL      │ — (brak time_val)                  │ summary.mysql.tables[].access_type │
  ├────────────┼────────────────────────────────────┼────────────────────────────────────┤
  │ Neo4j      │ summary.neo4j.total_db_hits        │ —                                  │
  └────────────┴────────────────────────────────────┴────────────────────────────────────┘

  Kod wyciąga dane ze wszystkich 4 baz (linia 416-443), ale potem na wykresie filtruje tylko PostgreSQL  
  (linia 457: edf[edf["database"] == "postgres"]). Tytuł wykresu to hardkodowane "EXPLAIN — czas
  wykonania zapytań SELECT (PostgreSQL)".

  Dlaczego tylko PostgreSQL na wykresie?

  To celowy wybór w kodzie, ale traci się przez to dużo wartościowych danych. Dane z MongoDB, MySQL i    
  Neo4j są w JSON-ach — po prostu nie są rysowane. Konkretne problemy:

  1. MySQL — time_val nigdy nie jest ustawiane (linia 420-425 nie ma gałęzi dla mysql), więc jest None.  
  Wynika to z tego, że MySQL EXPLAIN FORMAT=JSON nie zwraca execution_time_ms — podaje tylko estymowane  
  wiersze i typ dostępu.
  2. Neo4j — time_val jest ustawiane na total_db_hits, co jest inną jednostką niż ms. Nie da się tego na 
  jednym wykresie z PostgreSQL.
  3. MongoDB — ma execution_time_ms, więc mogłoby być rysowane obok PostgreSQL, ale jest odfiltrowane.   

  Co warto pokazać z tych danych?

  Mamy dużo więcej niż teraz wykorzystujemy. Oto co jest w JSON-ach a nie jest wizualizowane:

  1. Zmiana typu skanu (najcenniejsza informacja)

  Dane już są w JSON, np.:
  - S1 PostgreSQL: Seq Scan → Index Scan
  - S5 MongoDB: COLLSCAN → IXSCAN
  - S4 Neo4j: NodeByLabelScan → NodeIndexContainsScan

  Ale nie ma wykresu który to pokazuje. Tabela lub heatmapa ze zmianami scan type byłaby bardzo
  wartościowa.

  2. Liczba przejrzanych dokumentów/wierszy

  - S5 MongoDB: total_docs_examined: 500000 → 16 (31000x redukcja)
  - S1 MongoDB: total_docs_examined: 2000 → 126
  - S3 MySQL: rows_examined: 490120 → 95890

  To też nie jest wizualizowane, a świetnie pokazuje dlaczego indeksy pomagają.

  3. Porównanie execution_time między bazami

  PostgreSQL i MongoDB mają porównywalne execution_time_ms. Można je umieścić obok siebie.

  Podsumowanie — co poprawić?

  Obecna wizualizacja jest poprawna, ale niekompletna. Pokazuje tylko jeden wymiar (czas PostgreSQL) z   
  czterech dostępnych baz i pomija najciekawsze informacje (zmiana typu skanu, redukcja przejrzanych     
  wierszy).