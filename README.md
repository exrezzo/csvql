# CSVQL - Query CSV/TSV files with SQL

CSVQL è un tool Go che ti permette di esplorare e interrogare file CSV e TSV usando SQL, sfruttando SQLite come backend.

## Funzionalità

- **Scansione automatica**: Trova tutti i file CSV/TSV in una directory e sottodirectory
- **Database SQLite**: Crea automaticamente tabelle per ogni file
- **Supporto CSV e TSV**: Rileva automaticamente il delimitatore basandosi sull'estensione
- **Watch mode**: Monitora le modifiche in tempo reale e aggiorna il database
- **Query SQL complete**: JOIN, GROUP BY, aggregazioni, ORDER BY, ecc.
- **Persistenza**: Il database SQLite viene salvato e riutilizzato
- **Compatibile con DataGrip/DBeaver**: Collegati direttamente al file `.csvql.db`

## Installazione

```bash
# Installa globalmente
go install ./cmd/csvql

# Oppure compila localmente
go build -o csvql ./cmd/csvql
```

## Utilizzo

```bash
# Avvia e monitora una directory (watch mode automatico)
csvql -dir /path/to/data

# Query singola (esegue e esce)
csvql -dir /path/to/data -q "SELECT * FROM myfile LIMIT 10"

# Database path personalizzato
csvql -dir /path/to/data -db /tmp/mydata.db
```

### Output

```
CSVQL - CSV/TSV to SQLite
Database: /path/to/data/.csvql.db
Loaded 3 table(s):
  - employees (6 columns)
  - sales_orders (6 columns)
  - inventory_products (5 columns)

Watching for changes... (Ctrl+C to stop)

[UPDATE] /path/to/data/employees.csv
```

### Collegamento con DataGrip/DBeaver

1. Avvia csvql: `csvql -dir /path/to/data`
2. In DataGrip: **New Data Source** → **SQLite**
3. Seleziona il file: `/path/to/data/.csvql.db`
4. Le modifiche ai CSV si riflettono automaticamente nel database

## Esempi di query

```sql
-- Vedi tutti i dipendenti
SELECT * FROM employees;

-- Aggregazione per dipartimento
SELECT department, COUNT(*) as cnt, AVG(salary) as avg_salary 
FROM employees 
GROUP BY department;

-- Join tra tabelle
SELECT e.name, o.product, o.amount 
FROM employees e 
JOIN sales_orders o ON e.id = o.employee_id 
ORDER BY o.amount DESC;

-- Query su file TSV
SELECT * FROM inventory_products WHERE category = 'Electronics';
```

## Naming delle tabelle

I nomi delle tabelle vengono generati automaticamente dal path relativo:

| File | Tabella |
|------|---------|
| `data.csv` | `data` |
| `sales/orders.csv` | `sales_orders` |
| `2024-report.csv` | `_2024_report` |
| `my-file.tsv` | `my_file` |

## Struttura del progetto

```
csvql/
├── csvql.go           # API principale
├── csvql_test.go      # Test di integrazione
├── cmd/csvql/main.go  # CLI
├── loader/            # Parsing CSV/TSV
├── db/                # Gestione SQLite
├── watcher/           # File watching
└── testdata/          # Dati di esempio
```

## Test

```bash
go test ./...
```

## Dipendenze

- `github.com/mattn/go-sqlite3` - Driver SQLite
- `github.com/fsnotify/fsnotify` - File system watcher
