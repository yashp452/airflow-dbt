from airflow.decorators import dag, task
from datetime import datetime
from include.eczachly.scripts.snowpark.load_sec_filings_into_snowflake import load_sec_filings_to_snowflake
from include.eczachly.scripts.snowpark.load_earnings_transcripts_into_snowflake import load_earnings_transcripts_to_snowflake
from include.eczachly.scripts.snowpark.process_documents_with_cortex import process_documents_with_cortex_ai


@dag(
    "sec_filings_dag",
    description="Load SEC 10-K filings and PDFs into Snowflake stage",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
    },
    schedule="@weekly",
    catchup=False,
    tags=["community", "Zach Wilson", "SEC", "Snowflake"],
)
def sec_filings_dag():
    TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    @task(task_id="load_sec_filings_to_snowflake")
    def load_filings_task(tickers=None):
        """
        Load SEC 10-K filings and PDFs into Snowflake stage.
        
        Args:
            tickers: List of stock tickers to fetch filings for
        """
        if tickers is None:
            # Default tickers - you can customize this list
            tickers = TICKERS
        
        result = load_sec_filings_to_snowflake(tickers=tickers)
        print(f"Loaded {result['filings_loaded']} filings to {result['stage']}")
        return result

    @task(task_id="load_sec_filings_to_snowflake")
    def load_earnings_transcripts(tickers=None):
        """
        Load Earnings transcripts into Snowflake stage.
        Args:
            tickers: List of stock tickers to fetch filings for
        """
        if tickers is None:
            # Default tickers - you can customize this list
            tickers = TICKERS

        result = load_earnings_transcripts_to_snowflake(tickers=tickers)
        print(f"Loaded {result['filings_loaded']} transcripts to {result['stage']}")
        return result

    @task(task_id="process_with_cortex_ai")
    def process_documents_cortex(tickers=None):
        """
        Process documents with Cortex AI to extract paragraphs and create embeddings.
        Processes page by page and paragraph by paragraph, keeping images and tables connected.
        
        Args:
            tickers: List of stock tickers to process
        """
        if tickers is None:
            tickers = TICKERS
        
        result = process_documents_with_cortex_ai(tickers=tickers)
        print(f"Processed {result['sec_filings_processed']} SEC filings and {result['transcripts_processed']} transcripts")
        print(f"Chunks stored in: {result['chunks_table']}")
        return result

    # Execute tasks with dependencies
    filings = load_filings_task()
    transcripts = load_earnings_transcripts()
    
    # Process with Cortex AI after both filings and transcripts are loaded
    [filings, transcripts] >> process_documents_cortex()


sec_filings_dag()
