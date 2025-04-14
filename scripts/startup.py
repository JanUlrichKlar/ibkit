# scripts/startup.py

import os
from ibkit.importer.csv_processor import CSVProcessor

def main():
    # Resolve path to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    raw_dir = os.path.join(project_root, 'data', 'raw')
    output_dir = os.path.join(project_root, 'data', 'processed')

    print(f"Using RAW path:      {raw_dir}")
    print(f"Using OUTPUT path:   {output_dir}")

    processor = CSVProcessor(
        raw_dir=raw_dir,
        output_dir=output_dir
    )

    print("=== Saving yearly IBKR tables ===")
    processor.save_yearly_tables()

    print("\n=== Merging all years ===")
    merged_data = processor.merge_yearly_tables()

    print("\n=== Column summaries ===")
    processor.print_column_summaries(merged_data)

if __name__ == "__main__":
    main()


