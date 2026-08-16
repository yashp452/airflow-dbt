from ..jobs.polygon_spark_job import fetch_tickers, process_tickers

def test_fetch_tickers(requests_mock):
    url = "https://api.polygon.io/v3/reference/tickers?market=stocks&apiKey=your_polygon_api_key"
    mock_response = {
        "results": [
            {"ticker": "AAPL", "name": "Apple Inc."},
            {"ticker": "MSFT", "name": "Microsoft Corporation"}
        ],
        "status": "OK"
    }
    requests_mock.get(url, json=mock_response)

    tickers = fetch_tickers()

    assert len(tickers) == 2
    assert tickers[0]["ticker"] == "AAPL"

def test_process_tickers(spark):
    tickers = [
        {"ticker": "AAPL", "name": "Apple Inc."},
        {"ticker": "MSFT", "name": "Microsoft Corporation"}
    ]

    df = process_tickers(spark, tickers)
    result = df.collect()

    assert len(result) == 2
    assert result[0]['ticker'] == "AAPL"
    assert result[1]['name'] == "Microsoft Corporation"