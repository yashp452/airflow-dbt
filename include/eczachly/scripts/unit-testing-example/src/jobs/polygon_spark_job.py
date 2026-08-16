import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

API_KEY = "your_polygon_api_key"


def fetch_tickers():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&apiKey={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])


def process_tickers(spark, tickers):
    # Define schema (adjust based on Polygon fields)
    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("name", StringType(), True),
    ])

    # Convert to DataFrame
    df = spark.createDataFrame(tickers, schema=schema)
    return df

def main():
    spark = SparkSession.builder \
        .appName("PolygonTickersJob") \
        .getOrCreate()

    tickers = fetch_tickers()
    df = process_tickers(spark, tickers)

    # Example action: show or write to storage
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()