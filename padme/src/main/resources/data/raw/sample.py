import pandas as pd
import numpy as np
import hashlib
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw"
IN_DIR = BASE_DIR / "input"
OUT_DIR = BASE_DIR / "output"

RAW_PATH = RAW_DIR / "shuttle.csv"
OUT_TRAIN = IN_DIR / "shuttle_train.csv"
OUT_TEST = OUT_DIR / "shuttle_test.csv"

TRAIN_SIZE = 32000
TEST_SIZE = 8000
RANDOM_STATE = 42
TARGET_COL = "label"

IS_MULTI_CLASS = True
IS_REGRESSION = False

TRAIN_POS_RATIO = None
TEST_POS_RATIO = None

CATEGORICAL_COLS = []


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
    if IS_REGRESSION or TARGET_COL not in df.columns:
        return
    counts = df[TARGET_COL].value_counts().sort_index()
    ratios = df[TARGET_COL].value_counts(normalize=True).sort_index()
    print(f"\n{name} distribution:")
    print("Counts:", counts.to_dict())
    print("Ratio :", {k: round(v, 6) for k, v in ratios.to_dict().items()})


def print_regression_stats(name, df):
    if not IS_REGRESSION or TARGET_COL not in df.columns:
        return
    s = df[TARGET_COL]
    print(f"\n{name} target stats:")
    print(
        {
            "min": round(float(s.min()), 6),
            "max": round(float(s.max()), 6),
            "mean": round(float(s.mean()), 6),
            "std": round(float(s.std()), 6),
            "median": round(float(s.median()), 6),
        }
    )


def validate_ratio(name: str, ratio):
    if ratio is None:
        return
    if not (0.0 <= ratio <= 1.0):
        raise ValueError(f"{name} must be between 0 and 1, got {ratio}.")


def class_counts_from_ratio(size: int, pos_ratio: float):
    n1 = int(round(size * pos_ratio))
    n0 = size - n1
    return n0, n1


def stratified_sample_multiclass(df: pd.DataFrame, size: int, target_col: str, random_state: int) -> pd.DataFrame:
    counts = df[target_col].value_counts().sort_index()
    ratios = counts / len(df)

    expected = ratios * size
    target_counts = pd.Series(np.floor(expected), index=expected.index).astype(int)

    diff = size - target_counts.sum()
    if diff > 0:
        remainders = (expected - target_counts).sort_values(ascending=False)
        for cls in remainders.index[:diff]:
            target_counts[cls] += 1

    parts = []
    for cls, n in target_counts.items():
        df_cls = df[df[target_col] == cls]
        if len(df_cls) < n:
            raise ValueError(
                f"Not enough rows for class {cls}. Need {n}, have {len(df_cls)}."
            )
        if n > 0:
            parts.append(df_cls.sample(n=n, random_state=random_state))

    return pd.concat(parts, axis=0).sample(frac=1.0, random_state=random_state)


def random_sample(df: pd.DataFrame, size: int, random_state: int) -> pd.DataFrame:
    if len(df) < size:
        raise ValueError(f"Not enough rows. Need {size}, have {len(df)}.")
    return df.sample(n=size, random_state=random_state).sample(frac=1.0, random_state=random_state)


def build_label_encoders(df: pd.DataFrame, categorical_cols: list[str]) -> dict[str, dict[str, int]]:
    encoders = {}

    for col in categorical_cols:
        if col not in df.columns:
            continue

        values = df[col].astype("string").fillna("<NA>")
        unique_vals = pd.Index(values.unique()).sort_values()
        encoders[col] = {val: idx for idx, val in enumerate(unique_vals)}

    return encoders


def apply_label_encoders(
        df: pd.DataFrame,
        encoders: dict[str, dict[str, int]],
        categorical_cols: list[str]
) -> pd.DataFrame:
    out = df.copy()

    for col in categorical_cols:
        if col not in out.columns or col not in encoders:
            continue

        out[col] = (
            out[col]
            .astype("string")
            .fillna("<NA>")
            .map(encoders[col])
            .fillna(-1)
            .astype(int)
        )

    return out


def print_encoder_summary(encoders: dict[str, dict[str, int]]):
    if not encoders:
        print("\nNo categorical encoders were created.")
        return

    print("\nCategorical encoding summary:")
    for col, mapping in encoders.items():
        print(f"- {col}: {len(mapping)} categories")


IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

if IS_MULTI_CLASS and IS_REGRESSION:
    raise ValueError("IS_MULTI_CLASS and IS_REGRESSION cannot both be True.")

if not IS_MULTI_CLASS and not IS_REGRESSION:
    validate_ratio("TRAIN_POS_RATIO", TRAIN_POS_RATIO)
    validate_ratio("TEST_POS_RATIO", TEST_POS_RATIO)

df = load_df(RAW_PATH)

print("RAW rows:", len(df))
print("RAW cols:", df.shape[1])
print_balance("RAW", df)
print_regression_stats("RAW", df)

cols = df.columns.tolist()

if IS_REGRESSION:
    train = random_sample(
        df=df,
        size=TRAIN_SIZE,
        random_state=RANDOM_STATE
    )

elif IS_MULTI_CLASS:
    train = stratified_sample_multiclass(
        df=df,
        size=TRAIN_SIZE,
        target_col=TARGET_COL,
        random_state=RANDOM_STATE
    )

else:
    df_1 = df[df[TARGET_COL] == 1]
    df_0 = df[df[TARGET_COL] == 0]

    raw_counts = df[TARGET_COL].value_counts().to_dict()
    raw_pos_ratio = raw_counts.get(1, 0) / len(df)

    train_pos_ratio = raw_pos_ratio if TRAIN_POS_RATIO is None else TRAIN_POS_RATIO
    n0_train, n1_train = class_counts_from_ratio(TRAIN_SIZE, train_pos_ratio)

    if len(df_1) < n1_train or len(df_0) < n0_train:
        raise ValueError(
            f"Not enough rows per class for train. "
            f"Need {{0:{n0_train}, 1:{n1_train}}}, have {{0:{len(df_0)}, 1:{len(df_1)}}}."
        )

    train_1 = df_1.sample(n=n1_train, random_state=RANDOM_STATE)
    train_0 = df_0.sample(n=n0_train, random_state=RANDOM_STATE)
    train = pd.concat([train_0, train_1], axis=0).sample(frac=1.0, random_state=RANDOM_STATE)

print("\nTrain rows:", len(train))
print_balance("TRAIN", train)
print_regression_stats("TRAIN", train)

train_h = set(row_hash_series(train, cols))
all_h = row_hash_series(df, cols)

eligible_mask = ~all_h.isin(train_h)
eligible = df[eligible_mask]

print("\nEligible for test rows:", len(eligible))

if len(eligible) < TEST_SIZE:
    raise ValueError(f"Not enough eligible rows for test. Need {TEST_SIZE}, have {len(eligible)}.")

if IS_REGRESSION:
    test = random_sample(
        df=eligible,
        size=TEST_SIZE,
        random_state=RANDOM_STATE
    )

elif IS_MULTI_CLASS:
    test = stratified_sample_multiclass(
        df=eligible,
        size=TEST_SIZE,
        target_col=TARGET_COL,
        random_state=RANDOM_STATE
    )

else:
    raw_counts = df[TARGET_COL].value_counts().to_dict()
    raw_pos_ratio = raw_counts.get(1, 0) / len(df)

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

print("\nTest rows:", len(test))
print_balance("TEST", test)
print_regression_stats("TEST", test)

encoders = build_label_encoders(train, CATEGORICAL_COLS)
print_encoder_summary(encoders)

train = apply_label_encoders(train, encoders, CATEGORICAL_COLS)
test = apply_label_encoders(test, encoders, CATEGORICAL_COLS)

train.to_csv(OUT_TRAIN, index=False)
test.to_csv(OUT_TEST, index=False)

test_h = set(row_hash_series(test, cols))

print("\nTrain ∩ Test:", len(train_h & test_h))
print("Encoded categorical columns:", [c for c in CATEGORICAL_COLS if c in train.columns])
print("Final train cols:", train.shape[1])
print("Final test cols:", test.shape[1])
print("Done.")