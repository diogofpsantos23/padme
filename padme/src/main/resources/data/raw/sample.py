import pandas as pd
import hashlib
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw"
IN_DIR = BASE_DIR / "input"
OUT_DIR = BASE_DIR / "output"

RAW_PATH = RAW_DIR / "unsw_nb15.parquet"
OUT_TRAIN = IN_DIR / "unsw_nb15_train.csv"
OUT_TEST = OUT_DIR / "unsw_nb15_test.csv"

TRAIN_SIZE = 20000
TEST_SIZE = 4000
RANDOM_STATE = 42
TARGET_COL = "label"

TRAIN_POS_RATIO = 0.2
TEST_POS_RATIO = 0.2

def load_df(path: Path) -> pd.DataFrame:
    p = Path(path)
    ext = p.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(p)
    if ext == ".parquet":
        return pd.read_parquet(p)
    raise ValueError(f"Unsupported input format: {ext} (expected .csv or .parquet)")

def row_hash_series(df, cols):
    s = df[cols].astype("string").fillna("").agg("|".join, axis=1)
    return s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())

def print_balance(name, df):
    if TARGET_COL not in df.columns:
        return
    counts = df[TARGET_COL].value_counts().sort_index()
    ratios = df[TARGET_COL].value_counts(normalize=True).sort_index()
    print(f"\n{name} distribution:")
    print("Counts:", counts.to_dict())
    print("Ratio :", {k: round(v, 6) for k, v in ratios.to_dict().items()})

def validate_ratio(name: str, ratio):
    if ratio is None:
        return
    if not (0.0 <= ratio <= 1.0):
        raise ValueError(f"{name} must be between 0 and 1, got {ratio}.")

def class_counts_from_ratio(size: int, pos_ratio: float):
    n1 = int(round(size * pos_ratio))
    n0 = size - n1
    return n0, n1

IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

validate_ratio("TRAIN_POS_RATIO", TRAIN_POS_RATIO)
validate_ratio("TEST_POS_RATIO", TEST_POS_RATIO)

df = load_df(RAW_PATH)

print("RAW rows:", len(df))
print("RAW cols:", df.shape[1])
print_balance("RAW", df)

cols = df.columns.tolist()

raw_counts = df[TARGET_COL].value_counts().to_dict()
raw_pos_ratio = raw_counts.get(1, 0) / len(df)

df_1 = df[df[TARGET_COL] == 1]
df_0 = df[df[TARGET_COL] == 0]

n0_train, n1_train = class_counts_from_ratio(TRAIN_SIZE, TRAIN_POS_RATIO)

if len(df_1) < n1_train or len(df_0) < n0_train:
    raise ValueError(
        f"Not enough rows per class for train. "
        f"Need {{0:{n0_train}, 1:{n1_train}}}, have {{0:{len(df_0)}, 1:{len(df_1)}}}."
    )

train_1 = df_1.sample(n=n1_train, random_state=RANDOM_STATE)
train_0 = df_0.sample(n=n0_train, random_state=RANDOM_STATE)
train = pd.concat([train_0, train_1], axis=0).sample(frac=1.0, random_state=RANDOM_STATE)

train.to_csv(OUT_TRAIN, index=False)

print("\nTrain rows:", len(train))
print_balance("TRAIN", train)

train_h = set(row_hash_series(train, cols))
all_h = row_hash_series(df, cols)

eligible_mask = ~all_h.isin(train_h)
eligible = df[eligible_mask]

print("\nEligible for test rows:", len(eligible))

if len(eligible) < TEST_SIZE:
    raise ValueError(f"Not enough eligible rows for test. Need {TEST_SIZE}, have {len(eligible)}.")

test_pos_ratio = raw_pos_ratio if TEST_POS_RATIO is None else TEST_POS_RATIO
n0_test, n1_test = class_counts_from_ratio(TEST_SIZE, test_pos_ratio)

eligible_1 = eligible[eligible[TARGET_COL] == 1]
eligible_0 = eligible[eligible[TARGET_COL] == 0]

if len(eligible_1) < n1_test or len(eligible_0) < n0_test:
    raise ValueError(
        f"Not enough eligible rows per class for test. "
        f"Need {{0:{n0_test}, 1:{n1_test}}}, have {{0:{len(eligible_0)}, 1:{len(eligible_1)}}}."
    )

test_1 = eligible_1.sample(n=n1_test, random_state=RANDOM_STATE)
test_0 = eligible_0.sample(n=n0_test, random_state=RANDOM_STATE)

test = pd.concat([test_0, test_1], axis=0).sample(frac=1.0, random_state=RANDOM_STATE)
test.to_csv(OUT_TEST, index=False)

print("\nTest rows:", len(test))
print_balance("TEST", test)

test_h = set(row_hash_series(test, cols))

print("\nTrain ∩ Test:", len(train_h & test_h))
print("Done.")