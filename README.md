# REDCap Data Export Utility

![Python](https://img.shields.io/badge/python-3.6%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

`redcap_export.py` exports data from a REDCap project **directly from the MySQL database** while reproducing the structure of the REDCap UX **Raw Data Export**.

It was created to reliably export **large REDCap projects** where the built-in export tools can fail or become extremely slow due to very large `redcap_data` tables.

The exporter streams rows from MySQL and reconstructs the REDCap dataset incrementally, avoiding the heavy in-memory pivot used by the REDCap application.

---

# Features

* Export REDCap projects by **project ID**
* Output formats

  * CSV (default)
  * XML
* Export modes

  * Raw values
  * Human-readable labels
* Handles

  * Checkbox expansion (`field___code`)
  * Longitudinal events
  * Repeating instruments
  * DAG labels
* Automatically detects correct **`redcap_dataN` table**
* Streaming export for **very large datasets**
* Optional **gzip compression**
* Real-time **progress meter**
* Safe for **multi-million row projects**

---

# Why This Exists

Large REDCap projects can cause export failures due to the internal export pipeline:

1. Discover records
2. Build record/event grid
3. Pivot entire dataset in memory
4. Serialize output

Example project:

```
redcap_data rows: 11,654,655
project records: 261,798
fields: ~740
```

For datasets of this size, the built-in REDCap export may:

* run extremely slowly
* exhaust PHP memory
* return empty results

This exporter avoids those issues by:

```
streaming rows from MySQL
pivoting records incrementally
writing output immediately
```

---

# Requirements

* Python **3.6+**
* MySQL client installed
* MySQL credentials configured in:

```
~/.my.cnf
```

Example:

```
[client]
user=redcap_user
password=secret
host=localhost
```

The script uses the MySQL CLI so it automatically reads `.my.cnf`.

---

# Installation

Clone the repository:

```
git clone https://github.com/your-org/redcap-data-export.git
cd redcap-data-export
```

Make the script executable:

```
chmod +x redcap_export.py
```

---

# Usage

```
./redcap_export.py <project_id> [options]
```

Example:

```
./redcap_export.py 123
```

---

# Options

| Option                  | Description               | Default |
| ----------------------- | ------------------------- | ------- |
| `--format csv\|xml`     | Output format             | csv     |
| `--labels raw\|label`   | Raw values or labels      | raw     |
| `--gzip`                | Compress output           | off     |
| `--progress-interval N` | Progress update frequency | 2000    |

---

# Examples

Export project:

```
./redcap_export.py 123
```

Export with labels:

```
./redcap_export.py 123 --labels label
```

Export XML:

```
./redcap_export.py 123 --format xml
```

Compressed export:

```
./redcap_export.py 123 --gzip
```

---

# Output Files

Output filenames follow:

```
project_<PID>_export_<mode>.<format>
```

Examples:

```
project_123_export_raw_data.csv
project_123_export_labeled_data.csv
project_123_export_raw_data.xml
project_123_export_labeled_data.xml
```

Compressed output:

```
project_123_export_raw_data.csv.gz
```

---

# Example Output

```
REDCap Export Summary
----------------------
Project ID: 123
Database rows: 11,654,655
Project records: 261,798
Instruments: 22
Fields: 740
```

Progress meter:

```
Processed rows: 3,200,000 (27.5%) | 35,000 rows/sec | elapsed: 90s | ETA: 220s
```

---

# Performance

Typical export times:

| Dataset size | Runtime       |
| ------------ | ------------- |
| <1M rows     | seconds       |
| ~10M rows    | 2–5 minutes   |
| ~50M rows    | 10–15 minutes |

Memory usage stays low because data is **streamed rather than loaded entirely into memory**.

---

# Supported REDCap Structures

The exporter automatically handles:

* classic projects
* longitudinal projects
* repeating instruments
* repeating events
* checkbox fields
* DAG labels
* multiple `redcap_dataN` tables

---

# Limitations

The script reproduces **REDCap Raw Data Export** structure but does not yet replicate every internal REDCap export feature.

Not currently implemented:

* survey timestamp columns
* survey identifier fields
* certain external module exports

These can be added if needed.

---

# Repository Structure

```
redcap-data-export/
│
├─ redcap_export.py
├─ README.md
└─ LICENSE
```

---

# Security Notes

This script reads directly from the REDCap database.

Ensure:

* proper database permissions
* restricted server access
* secure handling of exported data

---

# Author

**Swaroop Samek**
March 2026
