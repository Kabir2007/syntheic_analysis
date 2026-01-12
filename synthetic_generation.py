import os
import math
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')  # Indian locale for Faker
Faker.seed(42)  # Reproducibility

INPUT_DIR = "data"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CONSTITUENCIES_FILE = f"{INPUT_DIR}/indian_constituencies.csv"
MIGRATION_OUT = f"{OUTPUT_DIR}/pc_migration_synthetic.csv"
SYSTEM_LOAD_OUT = f"{OUTPUT_DIR}/pc_system_load_synthetic.csv"
VOTER_DEMOGRAPHICS_OUT = f"{OUTPUT_DIR}/pc_voter_demographics_synthetic.csv"
SOURCE_LOG_OUT = f"{OUTPUT_DIR}/data_sources.txt"

# ================= LOAD CONSTITUENCIES =================

if not os.path.exists(CONSTITUENCIES_FILE):
    raise RuntimeError(
        f" Constituencies file not found: {CONSTITUENCIES_FILE}\n"
        f"   Run fetch_constituencies.py first to generate it."
    )

pc_df = pd.read_csv(CONSTITUENCIES_FILE)
TOTAL_PCS = len(pc_df)

print(f" Loaded {TOTAL_PCS} constituencies from {CONSTITUENCIES_FILE}")


# Based on Election Commission of India reports and parliamentary data
ANNUAL_FORM6_NATIONAL = 2_000_000  # Conservative estimate of annual Form 6 applications
TOTAL_VOTERS_INDIA = 968_000_000  # Approximate voter population (2024)
AVG_VOTERS_PER_PC = TOTAL_VOTERS_INDIA / 543  # ~1.78 million per constituency

AVG_FORM6_PER_PC = ANNUAL_FORM6_NATIONAL / TOTAL_PCS
ANCHOR_LEVEL = "ECI_aggregate_estimate"

print(f" Using realistic anchors:")
print(f"   - Avg voters per PC: {AVG_VOTERS_PER_PC:,.0f}")
print(f"   - Avg Form 6 per PC: {AVG_FORM6_PER_PC:,.0f}")

# ================= CATEGORY A: VOTER DEMOGRAPHICS =================

print("\n📈 Generating voter demographics...")

demographic_rows = []

for idx, row in pc_df.iterrows():
    pc_id = row['pc_id']
    state = row['state']
    constituency = row['constituency_name']
    
    # Use Faker for realistic variance in voter counts
    total_voters = fake.random_int(
        min=int(AVG_VOTERS_PER_PC * 0.5),
        max=int(AVG_VOTERS_PER_PC * 1.8)
    )
    
    # Gender distribution (realistic Indian ratios)
    male_ratio = fake.pyfloat(min_value=0.48, max_value=0.54)
    male_voters = int(total_voters * male_ratio)
    female_voters = total_voters - male_voters
    
    # Age distribution (using Faker for variation)
    age_18_25 = int(total_voters * fake.pyfloat(min_value=0.15, max_value=0.22))
    age_26_40 = int(total_voters * fake.pyfloat(min_value=0.30, max_value=0.38))
    age_41_60 = int(total_voters * fake.pyfloat(min_value=0.28, max_value=0.35))
    age_60_plus = total_voters - (age_18_25 + age_26_40 + age_41_60)
    
    # Literacy rate (varied by region)
    literacy_rate = fake.pyfloat(min_value=55.0, max_value=95.0)
    
    # Voter turnout (last election)
    turnout_rate = fake.pyfloat(min_value=45.0, max_value=85.0)
    
    demographic_rows.append({
        'pc_id': pc_id,
        'state': state,
        'constituency_name': constituency,
        'total_registered_voters': total_voters,
        'male_voters': male_voters,
        'female_voters': female_voters,
        'age_18_25': age_18_25,
        'age_26_40': age_26_40,
        'age_41_60': age_41_60,
        'age_60_plus': age_60_plus,
        'literacy_rate_percent': round(literacy_rate, 2),
        'last_election_turnout_percent': round(turnout_rate, 2),
        'data_origin': 'synthetic',
        'anchor_level': ANCHOR_LEVEL
    })

demo_df = pd.DataFrame(demographic_rows)
demo_df.to_csv(VOTER_DEMOGRAPHICS_OUT, index=False)
print(f" Saved to {VOTER_DEMOGRAPHICS_OUT}")

# ================= CATEGORY B: MIGRATION (Form 6 data) =================

print("\n Generating migration data...")

migration_rows = []

for idx, row in pc_df.iterrows():
    pc_id = row['pc_id']
    state = row['state']
    constituency = row['constituency_name']
    
    # Use Faker for realistic Form 6 request volumes
    base_form6 = fake.random_int(
        min=int(AVG_FORM6_PER_PC * 0.4),
        max=int(AVG_FORM6_PER_PC * 2.0)
    )
    
    # Inward vs outward migration (using realistic distributions)
    inward_ratio = fake.pyfloat(min_value=0.35, max_value=0.65)
    inward = int(base_form6 * inward_ratio)
    outward = base_form6 - inward
    net = inward - outward
    
    # Additional migrations (using Faker for variance)
    form_7_deletions = fake.random_int(
        min=int(base_form6 * 0.05),
        max=int(base_form6 * 0.15)
    )
    
    form_8_corrections = fake.random_int(
        min=int(base_form6 * 0.10),
        max=int(base_form6 * 0.25)
    )
    
    migration_rows.append({
        'pc_id': pc_id,
        'state': state,
        'constituency_name': constituency,
        'form6_addition_requests': base_form6,
        'form6_inward_migration': inward,
        'form6_outward_migration': outward,
        'net_migration': net,
        'form7_deletion_requests': form_7_deletions,
        'form8_correction_requests': form_8_corrections,
        'data_origin': 'synthetic',
        'anchor_level': ANCHOR_LEVEL
    })

migration_df = pd.DataFrame(migration_rows)
migration_df.to_csv(MIGRATION_OUT, index=False)
print(f" Saved to {MIGRATION_OUT}")

# ================= CATEGORY C: SYSTEM LOAD & PROCESSING =================

print("\n⚙️  Generating system load data...")

system_rows = []

for idx, mig_row in migration_df.iterrows():
    pc_id = mig_row['pc_id']
    state = mig_row['state']
    constituency = mig_row['constituency_name']
    
    total_requests = (
        mig_row['form6_addition_requests'] + 
        mig_row['form7_deletion_requests'] + 
        mig_row['form8_correction_requests']
    )
    
    # Use Faker for processing metrics
    approved = int(total_requests * fake.pyfloat(min_value=0.75, max_value=0.92))
    rejected = int(total_requests * fake.pyfloat(min_value=0.03, max_value=0.12))
    pending = max(0, total_requests - (approved + rejected))  # Ensure non-negative
    
    # Objections raised (using Faker)
    if total_requests > 0:
        objections = fake.random_int(
            min=max(0, int(total_requests * 0.01)),
            max=int(total_requests * 0.08)
        )
    else:
        objections = 0
    
    objections_resolved = int(objections * fake.pyfloat(min_value=0.60, max_value=0.95))
    objections_pending = max(0, objections - objections_resolved)  # Ensure non-negative
    
    # Processing time (realistic based on load)
    # Higher load = longer processing time
    load_factor = math.log(total_requests + 1)
    avg_processing_days = fake.pyfloat(
        min_value=max(5, load_factor * 3),
        max_value=min(90, load_factor * 8)
    )
    
    # Freeze period pending cases (pre-election freeze)
    if pending > 0:
        freeze_min = int(pending * 0.3)
        freeze_max = int(pending * 0.7)
        freeze_pending = fake.random_int(min=freeze_min, max=max(freeze_min, freeze_max))
    else:
        freeze_pending = 0
    
    # Officer workload
    officers_assigned = fake.random_int(min=2, max=12)
    cases_per_officer = round(total_requests / officers_assigned, 1)
    
    system_rows.append({
        'pc_id': pc_id,
        'state': state,
        'constituency_name': constituency,
        'total_requests': total_requests,
        'approved': approved,
        'rejected': rejected,
        'pending': pending,
        'objections_raised': objections,
        'objections_resolved': objections_resolved,
        'objections_pending': objections_pending,
        'freeze_period_pending': freeze_pending,
        'avg_processing_time_days': round(avg_processing_days, 1),
        'officers_assigned': officers_assigned,
        'cases_per_officer': cases_per_officer,
        'data_origin': 'synthetic',
        'anchor_level': ANCHOR_LEVEL
    })

system_df = pd.DataFrame(system_rows)
system_df.to_csv(SYSTEM_LOAD_OUT, index=False)
print(f" Saved to {SYSTEM_LOAD_OUT}")

# ================= SOURCES & METHODOLOGY LOG =================

print("\n Writing methodology documentation...")

with open(SOURCE_LOG_OUT, "w", encoding="utf-8") as f:
    f.write("=" * 70 + "\n")
    f.write("SYNTHETIC ELECTORAL DATA - SOURCES & METHODOLOGY\n")
    f.write("=" * 70 + "\n\n")
    
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total Constituencies: {TOTAL_PCS}\n\n")
    
    f.write("CONSTITUENCY NAMES:\n")
    f.write("-" * 70 + "\n")
    f.write("Source: Real Indian Lok Sabha constituencies\n")
    f.write(f"File: {CONSTITUENCIES_FILE}\n")
    f.write("Obtained via: Google Custom Search + Hardcoded verified list\n\n")
    
    f.write("AGGREGATE ANCHORS (Real Reference Points):\n")
    f.write("-" * 70 + "\n")
    f.write(f"1. Total Indian Voters (2024): ~{TOTAL_VOTERS_INDIA:,}\n")
    f.write(f"2. Annual Form 6 Applications: ~{ANNUAL_FORM6_NATIONAL:,}\n")
    f.write(f"3. Average Voters per Constituency: ~{int(AVG_VOTERS_PER_PC):,}\n")
    f.write(f"4. Average Form 6 per Constituency: ~{int(AVG_FORM6_PER_PC):,}\n")
    f.write("\nSources:\n")
    f.write("- Election Commission of India (ECI) reports\n")
    f.write("- Parliamentary answers on electoral reforms\n")
    f.write("- Census data (literacy, demographics)\n\n")
    
    f.write("SYNTHETIC DATA GENERATION METHOD:\n")
    f.write("-" * 70 + "\n")
    f.write("Library: Faker (Python) with Indian locale\n")
    f.write("Seed: 42 (for reproducibility)\n\n")
    
    f.write("Data Categories:\n")
    f.write("1. Voter Demographics:\n")
    f.write("   - Total voters: ±50-180% of national average\n")
    f.write("   - Gender ratio: 48-54% male (realistic Indian distribution)\n")
    f.write("   - Age groups: Realistic demographic pyramids\n")
    f.write("   - Literacy: 55-95% (varied by region)\n\n")
    
    f.write("2. Migration Data (Form 6/7/8):\n")
    f.write("   - Form 6 volume: ±40-200% of national average per PC\n")
    f.write("   - Inward/outward ratio: 35-65% realistic variance\n")
    f.write("   - Form 7 deletions: 5-15% of Form 6 volume\n")
    f.write("   - Form 8 corrections: 10-25% of Form 6 volume\n\n")
    
    f.write("3. System Load & Processing:\n")
    f.write("   - Approval rate: 75-92%\n")
    f.write("   - Rejection rate: 3-12%\n")
    f.write("   - Objections: 1-8% of total requests\n")
    f.write("   - Processing time: Log-scaled based on load (5-90 days)\n")
    f.write("   - Officer workload: 2-12 officers per constituency\n\n")
    
    f.write("CRITICAL DISCLAIMER:\n")
    f.write("=" * 70 + "\n")
    f.write("  ALL NUMERICAL VALUES ARE SYNTHETIC (FAKE)\n")
    f.write("  Only constituency names and state mappings are real\n")
    f.write("  Data is for research, testing, or demonstration purposes only\n")
    f.write("  Do NOT use for official analysis, policy-making, or publication\n")
    f.write("  For real data, consult Election Commission of India (eci.gov.in)\n\n")
    
    f.write("OUTPUT FILES:\n")
    f.write("-" * 70 + "\n")
    f.write(f"1. {VOTER_DEMOGRAPHICS_OUT}\n")
    f.write(f"2. {MIGRATION_OUT}\n")
    f.write(f"3. {SYSTEM_LOAD_OUT}\n")
    f.write(f"4. {SOURCE_LOG_OUT} (this file)\n\n")

print(f" Saved to {SOURCE_LOG_OUT}")

# ================= SUMMARY STATISTICS =================

print("\n" + "=" * 70)
print(" SUMMARY STATISTICS")
print("=" * 70)

print(f"\n  VOTER DEMOGRAPHICS:")
print(f"   Total voters (all PCs): {demo_df['total_registered_voters'].sum():,}")
print(f"   Average per PC: {demo_df['total_registered_voters'].mean():,.0f}")
print(f"   Male voters: {demo_df['male_voters'].sum():,}")
print(f"   Female voters: {demo_df['female_voters'].sum():,}")
print(f"   Avg literacy rate: {demo_df['literacy_rate_percent'].mean():.1f}%")
print(f"   Avg turnout: {demo_df['last_election_turnout_percent'].mean():.1f}%")

print(f"\n MIGRATION DATA:")
print(f"   Total Form 6 requests: {migration_df['form6_addition_requests'].sum():,}")
print(f"   Total inward migration: {migration_df['form6_inward_migration'].sum():,}")
print(f"   Total outward migration: {migration_df['form6_outward_migration'].sum():,}")
print(f"   Net migration: {migration_df['net_migration'].sum():,}")
print(f"   Form 7 deletions: {migration_df['form7_deletion_requests'].sum():,}")
print(f"   Form 8 corrections: {migration_df['form8_correction_requests'].sum():,}")

print(f"\n  SYSTEM LOAD:")
print(f"   Total requests processed: {system_df['total_requests'].sum():,}")
print(f"   Approved: {system_df['approved'].sum():,}")
print(f"   Rejected: {system_df['rejected'].sum():,}")
print(f"   Pending: {system_df['pending'].sum():,}")
print(f"   Objections raised: {system_df['objections_raised'].sum():,}")
print(f"   Avg processing time: {system_df['avg_processing_time_days'].mean():.1f} days")

print("\n All synthetic data generated successfully!")
print(f" Check the '{OUTPUT_DIR}' directory for output files")
