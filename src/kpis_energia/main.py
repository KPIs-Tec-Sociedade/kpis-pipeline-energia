import argparse
from databricks.sdk.runtime import spark


def main():
    parser = argparse.ArgumentParser(description="Pipeline KPIs Energia")
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark.sql(f"USE CATALOG {args.catalog}")
    print(f"Executando no catalogo: {args.catalog}")


if __name__ == "__main__":
    main()