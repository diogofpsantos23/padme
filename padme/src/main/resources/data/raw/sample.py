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

TRAIN_SIZE = 50000
TEST_SIZE = 2000
RANDOM_STATE = 42
TARGET_COL = "label"

TRAIN_POS_RATIO = 0.2

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

IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

df = load_df(RAW_PATH)

print("RAW rows:", len(df))
print("RAW cols:", df.shape[1])
print_balance("RAW", df)

cols = df.columns.tolist()

raw_counts = df[TARGET_COL].value_counts().to_dict()
raw_ratio_1 = raw_counts.get(1, 0) / len(df)

df_1 = df[df[TARGET_COL] == 1]
df_0 = df[df[TARGET_COL] == 0]

n1_train = int(round(TRAIN_SIZE * TRAIN_POS_RATIO))
n0_train = TRAIN_SIZE - n1_train

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

n1 = int(round(TEST_SIZE * raw_ratio_1))
n0 = TEST_SIZE - n1

eligible_1 = eligible[eligible[TARGET_COL] == 1]
eligible_0 = eligible[eligible[TARGET_COL] == 0]

if len(eligible_1) < n1 or len(eligible_0) < n0:
    raise ValueError(
        f"Not enough eligible rows per class for test. "
        f"Need {{0:{n0}, 1:{n1}}}, have {{0:{len(eligible_0)}, 1:{len(eligible_1)}}}."
    )

test_1 = eligible_1.sample(n=n1, random_state=RANDOM_STATE)
test_0 = eligible_0.sample(n=n0, random_state=RANDOM_STATE)

test = pd.concat([test_0, test_1], axis=0).sample(frac=1.0, random_state=RANDOM_STATE)
test.to_csv(OUT_TEST, index=False)

print("\nTest rows:", len(test))
print_balance("TEST", test)

test_h = set(row_hash_series(test, cols))

print("\nTrain ∩ Test:", len(train_h & test_h))
print("Done.")