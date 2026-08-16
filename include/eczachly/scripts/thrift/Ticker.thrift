namespace py dataschemas

struct Ticker {
  1: bool active,
  2: optional string cik,
  3: optional string composite_figi,
  4: optional string currency_name,
  5: optional string last_updated_utc,
  6: optional string locale,
  7: optional string market,
  8: optional string name,
  9: optional string primary_exchange,
  10: optional string share_class_figi,
  11: string ticker,
  12: optional string type,
}