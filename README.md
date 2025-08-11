# airflow-etl

- Input: `employee_raw_data.csv` (dirty)
- Transform: Python (clean, dedupe, split clean/rejects, metrics)
- Output (CSV only) → S3 partitioned by `dt={{ ds }}`:
  - `clean.csv`, `rejects.csv`, `metrics.json`

## 1) Requirements
- AWS account + S3 bucket (e.g., `my-etl-bucket`)
- EC2 Ubuntu 22.04 with IAM role.
- Security group: SSH (22) from own IP only

## 2) Bootstrap EC2
```bash
ssh -i ~/key.pem ubuntu@<EC2_IP>
sudo apt update -y
git clone https://github.com/<you>/employee-etl-airflow.git
cd employee-etl-airflow
bash scripts/bootstrap_airflow_ec2.sh
