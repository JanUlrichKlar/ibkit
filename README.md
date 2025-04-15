# 📊 IBKR CSV Importer (`ibkit`)

This project parses Interactive Brokers (IBKR) Activity Statement CSVs, extracts structured financial tables, and exports clean `.csv` and `.pkl` files for easy analysis in Python, MATLAB, or Excel.

---

## 🚀 Features

- 📥 Parses IBKR activity statements (`.csv`)
- 🧠 Splits into structured tables (`Trades`, `Transfers`, etc.)
- 📌 Auto-detects and labels by `Asset Category` (e.g., `Trades.Stocks`)
- 🧹 Cleans columns, removes blanks, and fixes column duplication
- 📁 Exports individual `.csv` tables and `.pkl` files
- 🔁 Merges all years into a single `ibkr_START_END.pkl` file

---

## 📂 Project Structure

```bash
ibkit/
├── ibkit/
│   └── importer/
│       └── csv_importer.py         # main parser logic
├── data/
│   ├── raw/                        # place IBKR CSVs here
│   └── processed/                  # output: CSVs + .pkl files
│       ├── ibkr_2021.pkl
│       ├── ibkr_2021/
│       │   ├── Trades_Stocks.csv
│       │   └── ...
│       └── ibkr_2021_2023.pkl      # merged all years
├── notebooks/                      # optional: demo notebooks
├── requirements.txt                # dependencies
├── README.md
├── LICENSE
└── .gitignore
