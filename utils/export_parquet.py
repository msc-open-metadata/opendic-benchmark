import argparse
import duckdb

def export(table_name:str, output_path:str, db_name:str):
    con = duckdb.connect(db_name)
    con.execute(f"""
        COPY (
          SELECT *
          FROM {table_name}
        ) TO '{output_path}' (FORMAT PARQUET)
    """)
    con.close()

def main():
    parser = argparse.ArgumentParser(description='Export data to Parquet')
    parser.add_argument('--table', type=str, help='Input table name')
    parser.add_argument('--output', type=str, help='Output Parquet file path')
    parser.add_argument('--db', type=str, help='Database name')
    args = parser.parse_args()

    export(args.table, args.output, args.db)
    print(f"{args.table} -> {args.output} ✔️")



if __name__ == '__main__':
    main()
