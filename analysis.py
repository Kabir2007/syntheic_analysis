import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path("output")

MIGRATION_FILE = BASE_DIR / "pc_migration_synthetic.csv"
SYSTEM_LOAD_FILE = BASE_DIR / "pc_system_load_synthetic.csv"
DEMOGRAPHIC_FILE = BASE_DIR / "pc_voter_demographics_synthetic.csv"

OUTPUT_FILE = BASE_DIR / "pc_final_metrics.csv"

migration_df = pd.read_csv(MIGRATION_FILE)
system_df = pd.read_csv(SYSTEM_LOAD_FILE)
demo_df = pd.read_csv(DEMOGRAPHIC_FILE)

# Merge all data on constituency identity
df = (
    demo_df
    .merge(migration_df, on=["pc_id", "state", "constituency_name"], how="left")
    .merge(system_df, on=["pc_id", "state", "constituency_name"], how="left")
)

def safe_div(a, b):
    """Safely divide two numbers, returning NaN if division is invalid."""
    return a / b if b not in (0, None, np.nan) else np.nan

def deviation_score(value, reference):
    if pd.isna(value) or pd.isna(reference) or reference == 0:
        return np.nan
    return 1 - abs(value - reference) / reference

def normalize_partial(scores, weights):
    valid = [(s, w) for s, w in zip(scores, weights) if not pd.isna(s)]
    if not valid:
        return np.nan
    total_weight = sum(w for _, w in valid)
    return sum(s * w for s, w in valid) / total_weight

# Gender ratio - ideal is 0.5 (1000 females per 1000 males = 50% female voters)
IDEAL_GENDER_RATIO = 0.5

# National averages for age distribution
national_age_avg = df[[
    "age_18_25", "age_26_40", "age_41_60", "age_60_plus"
]].mean()

# National average literacy rate
national_literacy_avg = df["literacy_rate_percent"].mean()

# National average turnout
national_turnout_avg = df["last_election_turnout_percent"].mean()

# National average Form 6 requests
national_form6_avg = df["form6_addition_requests"].mean()

# National average rejection rate
national_rejection_rate_avg = (df["rejected"] / df["total_requests"]).mean()

# National average objection rate
national_objection_rate_avg = (df["objections_raised"] / df["total_requests"]).mean()

# National maximum cases per officer (for normalization)
national_max_cases_per_officer = df["cases_per_officer"].max()


results = []

for _, row in df.iterrows():
    
    # ---------- STATISTICAL HEALTH SCORE ----------
    # Measures how well demographic indicators align with national norms

    # Gender Balance Score: Compare against ideal 50-50 ratio
    gender_ratio = safe_div(row["female_voters"], row["total_registered_voters"])
    GBS = deviation_score(gender_ratio, IDEAL_GENDER_RATIO)

    # Age Distribution Score: Compare age breakdown with national average
    age_vec = np.array([
        row["age_18_25"],
        row["age_26_40"],
        row["age_41_60"],
        row["age_60_plus"]
    ])
    age_avg_vec = national_age_avg.values
    ADS = 1 - np.nanmean(np.abs(age_vec - age_avg_vec))

    # Literacy Conformity Score: Compare with national average literacy
    LCS = deviation_score(
        row["literacy_rate_percent"],
        national_literacy_avg
    )

    # Turnout Alignment Score: Compare with national average turnout
    TAS = deviation_score(
        row["last_election_turnout_percent"],
        national_turnout_avg
    )

    # Combined Statistical Health Score (weighted average)
    SHS = normalize_partial(
        [GBS, ADS, LCS, TAS],
        [0.25, 0.30, 0.20, 0.25]
    )

    # ---------- MIGRATION PRESSURE INDEX ----------
    # Measures the scale and intensity of voter migration activity

    # Net Migration Intensity: Ratio of net migration to Form 6 requests
    NMI = safe_div(
        abs(row["net_migration"]),
        row["form6_addition_requests"]
    )

    # Form 6 Request Ratio: Compare Form 6 volume to national average
    F6R = safe_div(
        row["form6_addition_requests"],
        national_form6_avg
    )

    # Combined Migration Pressure Index (weighted average)
    MPI = normalize_partial(
        [NMI, F6R],
        [0.6, 0.4]
    )

    # ---------- ABUSE OF PROCESS SCORE ----------
    # Measures potential indicators of electoral roll manipulation or system stress

    # Rejection Rate Deviation: Compare rejection rate to national average
    rejection_rate = safe_div(row["rejected"], row["total_requests"])
    RRD = safe_div(
        rejection_rate,
        national_rejection_rate_avg
    )

    # Objection Deviation: Compare objection rate to national average
    objection_rate = safe_div(row["objections_raised"], row["total_requests"])
    ODD = safe_div(
        objection_rate,
        national_objection_rate_avg
    )

    # Administrative Load Pressure: Compare workload to national maximum
    ALP = safe_div(
        row["cases_per_officer"],
        national_max_cases_per_officer
    )

    # Combined Abuse of Process Score (weighted average)
    APS = normalize_partial(
        [RRD, ODD, ALP],
        [0.4, 0.35, 0.25]
    )

    # ---------- STORE RESULTS ----------
    
    results.append({
        "pc_id": row["pc_id"],
        "state": row["state"],
        "constituency_name": row["constituency_name"],
        "statistical_health_score": round(SHS, 4) if not pd.isna(SHS) else None,
        "migration_pressure_index": round(MPI, 4) if not pd.isna(MPI) else None,
        "abuse_of_process_score": round(APS, 4) if not pd.isna(APS) else None
    })

# ---------------- OUTPUT ----------------

final_df = pd.DataFrame(results)
final_df.to_csv(OUTPUT_FILE, index=False)

print(" Final constituency-level metrics computed successfully.")
print(f" Output saved to: {OUTPUT_FILE}")
print(f"\n National Benchmarks Used:")
print(f"   - Ideal Gender Ratio: {IDEAL_GENDER_RATIO:.4f} (50% female voters)")
print(f"   - National Literacy Rate: {national_literacy_avg:.2f}%")
print(f"   - National Turnout Rate: {national_turnout_avg:.2f}%")
print(f"   - National Avg Form 6 Requests: {national_form6_avg:.0f}")
print(f"   - National Rejection Rate: {national_rejection_rate_avg:.4f}")
print(f"   - National Objection Rate: {national_objection_rate_avg:.4f}")
