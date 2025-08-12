import os
import shutil
import pandas as pd
import pyarrow as pa
from deltalake.writer import write_deltalake

def excel_to_deltalake(file, input_folder, delta_base_dir):
    """
    Reads Excel file into DataFrame, writes to Delta Lake table (one table per file)
    inside the already-existing silver directory.
    """
    input_excel_path = os.path.join(input_folder, file)
    base_filename = os.path.splitext(file)[0]
    delta_table_dir = os.path.join(delta_base_dir, f"{base_filename}_delta_table")

    # Ensure the silver directory exists (do not create it)
    if not os.path.isdir(delta_base_dir):
        raise FileNotFoundError(f"Silver directory not found: {delta_base_dir}")

    # Validate Excel file exists
    if not os.path.isfile(input_excel_path):
        print(f"File not found: {input_excel_path}")
        return

    # Read Excel
    try:
        df = pd.read_excel(input_excel_path)
    except Exception as e:
        print(f"Failed to read Excel {input_excel_path}: {e}")
        return

    if df.empty:
        print(f"Excel file is empty: {input_excel_path}")
        return

    df = df.astype(str)  # Keep schema consistent
    table = pa.Table.from_pandas(df)

    # Create temp table directory inside silver
    tmp_table_dir = f"{delta_table_dir}_tmp"
    if os.path.exists(tmp_table_dir):
        shutil.rmtree(tmp_table_dir)

    # Write to temp directory
    write_deltalake(tmp_table_dir, table)

    # Replace existing delta table directory
    if os.path.exists(delta_table_dir):
        shutil.rmtree(delta_table_dir)
    os.rename(tmp_table_dir, delta_table_dir)

    print(f"Excel '{file}' ingested into Delta Lake at '{delta_table_dir}'")


def convert_all_excels_to_deltalake():
    raw_data_path = "/opt/airflow/data/bronze"
    delta_lake_folder = "/opt/airflow/data/silver"

    excel_files = [
        "shopping_mall_data.xlsx",
        "sales_dataset.xlsx",
        "customer_data.xlsx"
    ]

    for excel_file in excel_files:
        excel_to_deltalake(excel_file, raw_data_path, delta_lake_folder)


if __name__ == "__main__":
    convert_all_excels_to_deltalake()
