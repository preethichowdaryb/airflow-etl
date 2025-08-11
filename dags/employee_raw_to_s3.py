from datetime import datetime
import os, sys
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# allow importing from ../lib
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "lib"))
from employee_transform import run_transform

# Variables (override in UI → Admin → Variables)
INPUT_PATH    = Variable.get("emp_input_source", "/home/ubuntu/employee_raw_data.csv")
OUTPUT_BUCKET = Variable.get("emp_output_bucket", "my-etl-bucket")
OUTPUT_PREFIX = Variable.get("emp_output_prefix", "employee_etl")

with DAG(
    dag_id="employee_raw_to_s3",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",         # set to None for manual runs
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["etl","csv","s3"],
) as dag:

    @task
    def transform_local(ds=None):
        out_dir = f"/tmp/employee_etl/dt={ds}"
        return run_transform(INPUT_PATH, out_dir, ds)

    @task
    def upload_to_s3(paths: dict, ds=None):
        hook = S3Hook(aws_conn_id="aws_default")  # uses EC2 IAM role
        base = f"{OUTPUT_PREFIX}/dt={ds}"
        out = {}
        for name, local_path in paths.items():
            key = f"{base}/{os.path.basename(local_path)}"  # clean.csv, rejects.csv, metrics.json
            hook.load_file(filename=local_path, bucket_name=OUTPUT_BUCKET, key=key, replace=True)
            out[name] = f"s3://{OUTPUT_BUCKET}/{key}"
        return out

    upload_to_s3(transform_local())

