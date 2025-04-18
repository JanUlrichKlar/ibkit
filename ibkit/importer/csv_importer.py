import csv
import re
import pickle
import pandas as pd
from pathlib import Path
import shutil
from collections import defaultdict


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

    def extract_section(self, rows):
        header_indices = [(i, row[0].strip()) for i, row in enumerate(rows)
                          if len(row) > 1 and isinstance(row[1], str) and row[1].strip().lower() == "header"]

        section_ranges = []
        section_names = []

        for idx, (start_idx, section) in enumerate(header_indices):
            end_idx = header_indices[idx + 1][0] if idx + 1 < len(header_indices) else len(rows)
            section_ranges.append([start_idx, end_idx])
            section_names.append([section, None])

        seen = defaultdict(list)
        for i, (section, _) in enumerate(section_names):
            seen[section].append(i)

        for section, dup_indices in seen.items():
            for idx in dup_indices:
                start_idx, end_idx = section_ranges[idx]
                content_rows = rows[start_idx + 1:end_idx]
                asset_category = None
                for r in content_rows:
                    if section == "Transfers" and len(r) > 2:
                        asset_candidate = r[2]
                    elif section in ["Trades", "Open Positions", "Financial Instrument Information"] and len(r) > 3:
                        asset_candidate = r[3]
                    else:
                        asset_candidate = None
                    if asset_candidate and "Total" not in asset_candidate:
                        asset_category = asset_candidate.strip()
                        break
                section_names[idx][1] = asset_category or None

        grouped = defaultdict(dict)

        for idx, ((start_idx, end_idx), name_pair) in enumerate(zip(section_ranges, section_names)):
            section, asset_category = name_pair
            raw_headers = rows[start_idx][1:]
            headers = [h.strip() for h in raw_headers if h.strip() != ""]
            content_rows = rows[start_idx + 1:end_idx]
            table_rows = [r[1:1 + len(headers)] for r in content_rows if len(r) > 1]
            df = pd.DataFrame(table_rows, columns=headers).dropna(how="all")

            if df.empty:
                continue

            df = df.reset_index(drop=True)
            grouped[section][asset_category or "General"] = df
            print(f"📦 Section processed: {section}_{asset_category or 'General'} → {df.shape}")

        return grouped

    def process_all(self, output_dir="data/processed"):
        project_root = Path(__file__).resolve().parents[2]
        output_dir = (project_root / output_dir).resolve()

        if output_dir.exists():
            shutil.rmtree(output_dir)
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
            tables = self.extract_section(rows)

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
                name = name.replace(" ", "_").replace(".", "_")
                name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
                name = name.replace("_Header", "")
                name = re.sub(r"_+", "_", name)
                name = name.strip("_")
                return name

            for section, category_dict in tables.items():
                for category, df in category_dict.items():
                    name = section if category == "General" else f"{section}_{category}"
                    safe_name = sanitize_filename(name)
                    df.columns = dedup_columns(df.columns)

                    if "Qty" in df.columns:
                        df.rename(columns={"Qty": "Quantity"}, inplace=True)

                    df = df.loc[:, df.columns.map(lambda col: isinstance(col, str) and col.strip() != "")]
                    df = df.copy()
                    df = df.replace(r"^\s*$", pd.NA, regex=True)
                    df = df.dropna(axis=1, how="all")

                    if not df.empty:
                        output_file = output_subdir / f"{safe_name}.csv"
                        try:
                            df.to_csv(output_file, index=False)
                            print(f"✅ Saved: {output_file.name} → {df.shape}")
                        except OSError as e:
                            print(f"❌ Failed to save table '{category}': {e}")

            # Write nested pickle structure with section[category] layout
            pkl_path = output_dir / f"ibkr_{year}.pkl"
            with open(pkl_path, "wb") as f:
                pickle.dump(tables, f)
            print(f"💾 Saved all tables to: {pkl_path}")
            print(f"✅ {sum(len(v) for v in tables.values())} tables saved for {year}.")

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






































