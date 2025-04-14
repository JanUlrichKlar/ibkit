# ibkit/importer/csv_importer.py

import csv
import re
import pickle
import pandas as pd
from pathlib import Path


class CSVImporter:
    def __init__(self, data_dir):
        project_root = Path(__file__).resolve().parents[2]
        self.data_dir = (project_root / data_dir).resolve()

    def list_csv_files(self):
        return [f.name for f in self.data_dir.glob("*.csv")]

    def load_file(self, filepath):
        rows = []
        max_len = 0

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=",", quotechar='"', skipinitialspace=True)
            for raw in reader:
                if len(raw) == 1 and "," in raw[0] and '"' in raw[0]:
                    raw = next(csv.reader([raw[0]], delimiter=",", quotechar='"', skipinitialspace=True))
                clean_row = []
                for cell in raw:
                    cell = cell.strip()
                    if " - Held with" in cell:
                        cell = re.split(r" - Held with", cell, maxsplit=1)[0].strip()
                    clean_row.append(cell)
                rows.append(clean_row)
                max_len = max(max_len, len(clean_row))

        for row in rows:
            while len(row) < max_len:
                row.append("")
            for i in reversed(range(len(row))):
                if row[i].strip():
                    row[i] = row[i].rstrip(";")
                    break

        return rows

    def extract_info_sections(self, rows):
        subtables = {}
        current_section = None
        current_headers = None
        current_rows = []

        for row in rows:
            if len(row) > 1 and isinstance(row[1], str) and row[1].strip().lower() == "header":
                if current_section and current_headers and current_rows:
                    self._process_section(current_section, current_headers, current_rows, subtables)
                current_section = row[0].strip()
                raw_headers = row[1:]
                current_headers = [h.strip() for h in raw_headers if h.strip() != ""]
                current_rows = []
            elif current_section and current_headers:
                current_rows.append(row[1:1 + len(current_headers)])

        if current_section and current_headers and current_rows:
            self._process_section(current_section, current_headers, current_rows, subtables)

        return subtables

    def _process_section(self, section_name, headers, rows, target_dict):
        clipped_rows = [row[:len(headers)] for row in rows]
        df = pd.DataFrame(clipped_rows, columns=headers)
        df = df.dropna(how='all')

        split_by_asset = {"Trades", "Transfers", "Open Positions"}

        if section_name in split_by_asset and "Asset Category" in df.columns:
            for category, group in df.groupby("Asset Category"):
                key = f"{section_name}.{category.strip()}"
                key = key.replace(" ", "_")
                target_dict[key] = group.reset_index(drop=True)
        else:
            key = section_name
            suffix = 1
            while key in target_dict:
                key = f"{section_name}_{suffix}"
                suffix += 1
            target_dict[key] = df.reset_index(drop=True)

    def process_all(self, output_dir="data/processed"):
        project_root = Path(__file__).resolve().parents[2]
        output_dir = (project_root / output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        files = self.list_csv_files()
        if not files:
            print("❌ No CSV files found.")
            return

        processed_years = []

        for fname in files:
            year_matches = re.findall(r"\d{4}", fname)
            if not year_matches:
                print(f"⚠️ Skipping file (no 4-digit year found): {fname}")
                continue

            year = year_matches[-1]
            processed_years.append(year)
            output_subdir = output_dir / f"ibkr_{year}"
            output_subdir.mkdir(parents=True, exist_ok=True)

            print(f"\n📂 Processing {fname} → saving to: {output_subdir}")

            file_path = self.data_dir / fname
            rows = self.load_file(file_path)
            tables = self.extract_info_sections(rows)

            if not tables:
                print("⚠️ No tables found.")
                continue

            def dedup_columns(columns):
                seen = {}
                result = []
                for col in columns:
                    if col in seen:
                        seen[col] += 1
                        result.append(f"{col}_{seen[col]}")
                    else:
                        seen[col] = 0
                        result.append(col)
                return result

            def sanitize_filename(name):
                name = name.replace(" ", "_")
                return re.sub(r"[^a-zA-Z0-9_]", "_", name)

            for name, df in tables.items():
                safe_name = sanitize_filename(name)
                df.columns = dedup_columns(df.columns)
                df = df.loc[:, df.columns.map(lambda col: isinstance(col, str) and col.strip() != "")]
                df = df.copy()
                df = df.replace(r"^\s*$", pd.NA, regex=True)
                df = df.dropna(axis=1, how="all")

                output_file = output_subdir / f"{safe_name}.csv"
                try:
                    df.to_csv(output_file, index=False)
                except OSError as e:
                    print(f"❌ Failed to save table '{name}': {e}")

            pkl_path = output_dir / f"ibkr_{year}.pkl"
            with open(pkl_path, "wb") as f:
                pickle.dump(tables, f)
            print(f"💾 Saved all tables to: {pkl_path}")
            print(f"✅ {len(tables)} tables saved for {year}.")

        # 🧩 Merge all years into one file
        processed_years = sorted(set(processed_years))
        if processed_years:
            merged = {}
            for year in processed_years:
                with open(output_dir / f"ibkr_{year}.pkl", "rb") as f:
                    merged[year] = pickle.load(f)

            start_year = processed_years[0]
            end_year = processed_years[-1]
            merged_file = output_dir / f"ibkr_{start_year}_{end_year}.pkl"
            with open(merged_file, "wb") as f:
                pickle.dump(merged, f)

            print(f"\n🧩 Merged all years into: {merged_file}")


def main():
    importer = CSVImporter("data/raw")
    importer.process_all()


if __name__ == "__main__":
    main()













