# 📊 IBKR CSV Importer (`ibkit`)

This project parses Interactive Brokers (IBKR) Activity Statement CSVs, extracts structured financial tables, and exports clean `.csv`, `.json`, `.pkl`, or `.xlsx` files for easy analysis in Python, MATLAB, or Excel.

---

## 🚀 Features

- 📥 Parses IBKR activity statements (`.csv`)
- 🧠 Splits into structured tables (`Trades`, `Transfers`, etc.)
- 📌 Auto-detects and labels by `Asset Category` (e.g., `Trades.Stocks`)
- 🧹 Cleans columns, removes blanks, and fixes column duplication
- 📁 Exports tables to:
  - individual `.csv` files
  - `.json` files
  - `.xlsx` workbook (one sheet per table)
  - `.pkl` files (nested dictionary structure)
- 🔁 Merges all years into a single file (`.pkl` or `.json`) for all tables combined
- ⚙️ Configurable output via `export_format` parameter (`pkl`, `json`, `csv`, `excel`, or `all`)

---

## 📂 Project Structure

```bash
ibkit/
├── ibkit/
│   └── importer/
│       └── csv_importer.py         # main parser logic
├── data/
│   ├── raw/                        # place IBKR CSVs here
│   └── processed/                  # output: CSVs + .pkl/.json/.xlsx files
│       ├── ibkr_2021.pkl
│       ├── ibkr_2021.json
│       ├── ibkr_2021.xlsx
│       ├── ibkr_2021/
│       │   ├── Trades_Stocks.csv
│       │   └── ...
│       └── ibkr_2021_2023.pkl      # merged all years
│       └── ibkr_2021_2023.json     # merged all years
├── notebooks/                      # optional: demo notebooks
├── requirements.txt                # dependencies
├── README.md
├── LICENSE
└── .gitignore
```

---

## 🔧 Usage

You can control the export format using the `export_format` argument in `CSVImporter.process_all()`:

```python
from ibkit.importer.csv_importer import CSVImporter

importer = CSVImporter("data/raw")
importer.process_all(export_format="all")  # or "pkl", "json", "csv", "excel"
```

---
