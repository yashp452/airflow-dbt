import os

import pandas as pd

from include.eczachly.snowflake_queries import get_snowpark_session


def load_csv():
    schema = os.getenv("SCHEMA") or os.getenv("STUDENT_SCHEMA", "bootcamp")
    session = get_snowpark_session(schema=schema)

    pdf = pd.read_csv(
        "include/eczachly/scripts/snowpark/zachs_posts.csv",
        sep=",",  # explicit delimiter
        engine="python",  # tolerant of embedded newlines in quoted fields
        quotechar='"',  # " ... " encloses a field
        doublequote=True,  # ""  -> literal "   (matches your sample)
        skip_blank_lines=False,  # keep any intentional empty lines inside quotes
        parse_dates=["Date"],
        on_bad_lines='skip',
        dtype={
            "ShareLink": "string",
            "SharedURL": "string",
            "MediaURL": "string",
            "Visibility": "string",
        },
        keep_default_na=False,  # don’t turn empty cells into NaN unless you want to
    )

    # Convert Date to ISO string so Snowpark can infer the type (same as key_authentication script)
    if "Date" in pdf.columns:
        pdf["Date"] = pdf["Date"].dt.strftime("%Y-%m-%d")

    # create_dataframe() accepts list of dicts, not pandas DataFrame directly
    rows = pdf.to_dict(orient="records")
    df_snow = session.create_dataframe(rows)
    table_name = f"{schema}.linkedin_shares"
    df_snow.write.mode("overwrite").save_as_table(table_name)

    print(f"✅ Loaded {len(pdf):,} rows from 'zachs_posts.csv' into {table_name}")
    session.close()








if __name__ == "__main__":
    load_csv()
