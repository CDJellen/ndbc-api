import polars as pl

data = [{'a': None} for _ in range(200)] + [{'a': 1.5}]
try:
    df = pl.DataFrame(data)
    print("Default worked")
except Exception as e:
    print("Default failed:", e)

try:
    df = pl.DataFrame(data, infer_schema_length=None)
    print("infer_schema_length=None worked")
except Exception as e:
    print("infer_schema_length=None failed:", e)
