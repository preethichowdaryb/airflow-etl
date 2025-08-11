import pandas as pd, numpy as np, json, os

DEPT_MAP = {
  "engineering":"Engineering","eng":"Engineering",
  "analytics":"Analytics","product":"Product",
  "sales":"Sales","hr":"HR","finance":"Finance"
}

def run_transform(input_path: str, out_dir: str, run_ds: str) -> dict:
    # 1) read raw CSV
    df = pd.read_csv(input_path)

    # 2) tidy text
    for c in ["first_name","last_name","email","department","city","state_province","country"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # 3) standardize department
    if "department" in df.columns:
        norm = df["department"].str.lower().str.replace(r"[^a-z]", "", regex=True)
        df["department_final"] = norm.map(DEPT_MAP).fillna(df["department"])
    else:
        df["department_final"] = np.nan

    # 4) cast numbers & dates
    df["salary_usd"] = pd.to_numeric(df.get("salary_usd", np.nan), errors="coerce")
    df["age"]        = pd.to_numeric(df.get("age", np.nan), errors="coerce")
    for dc in ["hire_date","birth_date","last_promotion_date","termination_date"]:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce")

    # 5) dedupe
    if "employee_id" in df.columns:
        if "hire_date" in df.columns:
            df = df.sort_values(["employee_id","hire_date"], ascending=[True, False])
        df = df.drop_duplicates(subset=["employee_id"], keep="first")
    df = df.drop_duplicates()

    # 6) split clean vs rejects
    required_ok = df["employee_id"].notna() if "employee_id" in df else False
    email_ok    = df["email"].str.contains("@", na=False) if "email" in df else False
    dept_ok     = df["department_final"].notna()
    dates_ok    = (df["hire_date"].notna() & df["birth_date"].notna()) if "hire_date" in df and "birth_date" in df else True
    good_mask   = required_ok & email_ok & dept_ok & dates_ok

    clean   = df[good_mask].copy()
    rejects = df[~good_mask].copy()

    # 7) write outputs (CSV only)
    os.makedirs(out_dir, exist_ok=True)
    clean_path   = f"{out_dir}/clean.csv"
    rejects_path = f"{out_dir}/rejects.csv"
    metrics_path = f"{out_dir}/metrics.json"

    clean.to_csv(clean_path, index=False)
    rejects.to_csv(rejects_path, index=False)
    with open(metrics_path, "w") as f:
        json.dump({"run_ds": run_ds, "total": int(len(df)),
                   "clean": int(len(clean)), "rejects": int(len(rejects))}, f)

    return {"clean": clean_path, "rejects": rejects_path, "metrics": metrics_path}

