import csv
import re
import pickle
import json
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
                # Special handling for Net Asset Value section
                if section == "Net Asset Value" and len(rows[start_idx]) > 2:
                    # Check if "Time Weighted Rate of Return" is in the 3rd position of the header row
                    if rows[start_idx][2].strip() == "Time Weighted Rate of Return":
                        section_names[idx][1] = "wRR"
                        continue

                for r in content_rows:
                    if section in ["Transfers", "Financial Instrument Information"] and len(r) > 2:
                        asset_candidate = r[2]
                    elif section in ["Trades", "Open Positions"] and len(r) > 3:
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

            for col in df.columns:
                try:
                    cleaned = df[col].astype(str).str.replace(",", "", regex=False)
                    numeric_col = pd.to_numeric(cleaned, errors="coerce")
                    if numeric_col.notna().sum() > 0 and numeric_col.count() >= 0.5 * len(df):
                        df[col] = numeric_col
                except Exception:
                    pass

            df = df.reset_index(drop=True)
            grouped[section][asset_category or "General"] = df
            print(f"📦 Section processed: {section}_{asset_category or 'General'} → {df.shape}")

        return grouped

    def convert_date_columns(self, df):
        """Convert all columns containing 'date' (case insensitive) to datetime format."""
        if df.empty:
            return df
        
        # Find all columns containing 'date' (case insensitive)
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        
        for col in date_columns:
            try:
                # Handle empty strings by converting them to pandas NA (Not Available)
                df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
                # Convert to datetime, using 'coerce' to handle invalid dates by converting them to NaT
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"✓ Converted {col} to datetime in table shape {df.shape}")
            except Exception as e:
                print(f"⚠️ Failed to convert {col} to datetime: {e}")
        
        return df

    def prepare_for_json(self, df_dict):
        """Convert DataFrame dictionary to JSON-serializable format.
        
        Args:
            df_dict: Dictionary containing DataFrame records
            
        Returns:
            List of dictionaries with values converted to JSON-serializable format
        """
        def convert_value(v):
            # Handle NaN and NaT values by converting to None
            if pd.isna(v) or isinstance(v, pd._libs.tslibs.nattype.NaTType):
                return None
            # Convert pandas Timestamp objects to ISO format strings
            if isinstance(v, pd.Timestamp):
                return v.isoformat()
            return v

        return [{k: convert_value(v) for k, v in record.items()} for record in df_dict]

    def fix_trades_subtotals_account(self, df):
        """Fix subtotal rows in trades data by shifting Account value one cell right.
        
        In subtotal rows, the Account column actually contains Symbol data.
        This method moves that value to the correct column.
        
        Args:
            df: DataFrame containing trades data
            
        Returns:
            DataFrame with corrected subtotal rows
        """
        if not {'Account', 'Header'}.issubset(df.columns) or df.empty:
            return df

        df = df.copy()
        # Find rows where Header contains 'SubTotal' (case insensitive)
        subtotal_mask = df['Header'].str.contains('SubTotal', na=False, case=False)

        # For these rows, if Account has a value, shift it one column right
        cols = list(df.columns)
        account_idx = cols.index('Account')
        if account_idx + 1 < len(cols):
            next_col = cols[account_idx + 1]
            # Store Account value and shift it to the next column
            subtotal_rows = df[subtotal_mask].copy()
            df.loc[subtotal_mask, next_col] = subtotal_rows['Account']
            df.loc[subtotal_mask, 'Account'] = pd.NA

        return df

    def process_all(self, output_dir="data/processed", export_format="pkl"):
        export_format = export_format.lower()
        allowed_formats = {"pkl", "json", "csv", "excel", "all"}
        if export_format not in allowed_formats:
            raise ValueError(f"Invalid export format: {export_format}. Must be one of {allowed_formats}")

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

            xls_path = output_dir / f"ibkr_{year}.xlsx"
            json_path = output_dir / f"ibkr_{year}.json"
            pkl_path = output_dir / f"ibkr_{year}.pkl"
            json_dict = {}

            writer = pd.ExcelWriter(xls_path, engine="openpyxl") if export_format in {"excel", "all"} else None

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
                    
                    # Convert date columns to datetime
                    df = self.convert_date_columns(df)
                    
                    # Fix trades subtotals and update the df in tables dictionary
                    if section == "Trades":
                        df = self.fix_trades_subtotals_account(df)
                        tables[section][category] = df  # Update the tables dictionary with fixed df

                    if not df.empty:
                        if export_format in {"csv", "all"}:
                            output_file = output_subdir / f"{safe_name}.csv"
                            try:
                                df.to_csv(output_file, index=False)
                                print(f"✅ Saved: {output_file.name} → {df.shape}")
                            except OSError as e:
                                print(f"❌ Failed to save table '{category}': {e}")

                        if writer:
                            try:
                                df.to_excel(writer, sheet_name=safe_name[:31], index=False)
                            except Exception as e:
                                print(f"❌ Failed to write sheet '{safe_name}': {e}")

                        if export_format in {"json", "all"}:
                            json_dict[safe_name] = self.prepare_for_json(df.to_dict(orient="records"))

            if writer:
                writer.close()

            if export_format in {"json", "all"}:
                with open(json_path, "w", encoding="utf-8") as jf:
                    json.dump(json_dict, jf, indent=2, ensure_ascii=False)

            if export_format in {"pkl", "all"}:
                with open(pkl_path, "wb") as f:
                    pickle.dump(tables, f)
                print(f"💾 Saved all tables to: {pkl_path}")

            print(f"✅ {sum(len(v) for v in tables.values())} tables saved for {year}.")

        processed_years = sorted(set(processed_years))
        if processed_years and export_format in {"pkl", "json", "all"}:
            merged = {}
            for year in processed_years:
                with open(output_dir / f"ibkr_{year}.pkl", "rb") as f:
                    merged[year] = pickle.load(f)

            start_year = processed_years[0]
            end_year = processed_years[-1]
            merged_file = output_dir / f"ibkr_{start_year}_{end_year}.pkl"
            merged_json_path = output_dir / f"ibkr_{start_year}_{end_year}.json"

            if export_format in {"pkl", "all"}:
                with open(merged_file, "wb") as f:
                    pickle.dump(merged, f, protocol=4)
                print(f"\n🧩 Merged all years into: {merged_file}")

            if export_format in {"json", "all"}:
                merged_dict = {
                    year: {
                        f"{section}_{cat}" if cat != "General" else section: self.prepare_for_json(
                            df.to_dict(orient="records"))
                        for section, catmap in year_dict.items()
                        for cat, df in catmap.items()
                    } for year, year_dict in merged.items()
                }
                with open(merged_json_path, "w", encoding="utf-8") as jf:
                    json.dump(merged_dict, jf, indent=2, ensure_ascii=False)
                print(f"🧾 Exported merged data to: {merged_json_path}")

def main():
    importer = CSVImporter("data/raw")
    importer.process_all(export_format="all")


if __name__ == "__main__":
    main()