from pathlib import Path

import click
import requests
import polars as pl


schema = {
    'VendorID': pl.Int64, 
    'store_and_fwd_flag': pl.String, 
    'RatecodeID': pl.Int64, 
    'PULocationID': pl.String, 
    'DOLocationID': pl.String, 
    'passenger_count': pl.Int64, 
    'trip_distance': pl.Float64, 
    'fare_amount': pl.Float64, 
    'extra': pl.Float64, 
    'mta_tax': pl.Float64, 
    'tip_amount': pl.Float64,
    'tolls_amount': pl.Float64, 
    'ehail_fee': pl.String, 
    'improvement_surcharge': pl.Float64, 
    'total_amount': pl.Float64, 
    'payment_type': pl.String, 
    'trip_type': pl.Int64, 
    'congestion_surcharge': pl.String,
}

TF_SERVING_URL = "http://localhost:8501/v1/models/tip_model:predict"


@click.command(name="pred")
@click.option("--input", "input_path", required=True, type=click.Path(exists=True), help="Input CSV file Path")
@click.option("--output", "output_path", required=True, type=click.Path(), help="Output CSV file path")
def predict(input_path, output_path):
    input_path = Path(input_path)
    output_path = Path(output_path)

    df = pl.read_csv(
        input_path,
        schema_overrides=schema,
        try_parse_dates=True,
    )

    ml_df = df.select(
        pl.col('passenger_count').cast(pl.Float64),
        pl.col('PULocationID'),
        pl.col('DOLocationID'),
        pl.col('payment_type'),
        pl.col('fare_amount'),
        pl.col('tolls_amount'),
        pl.col('trip_distance'),
    )

    instances = ml_df.to_dicts()

    click.echo(f"Sending {len(instances)} rows to TF serving...")
    response = requests.post(TF_SERVING_URL, json={"instances": instances})
    response.raise_for_status()

    preds = response.json()["predictions"]
    pred_vals = [p[0] if isinstance(p, list) else p for p in preds]

    result_df = df.with_columns(pl.Series(name="predicted_tip_amount", values=pred_vals))

    result_df.write_csv(output_path)
    click.echo(f"Wrote {len(result_df)} rows to {output_path}")


if __name__ == "__main__":
    predict()
