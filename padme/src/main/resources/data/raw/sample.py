import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw"
IN_DIR = BASE_DIR / "input"
OUT_DIR = BASE_DIR / "output"

RAW_PATH = RAW_DIR / "unsw_nb15.parquet"
OUT_TRAIN = IN_DIR / "unsw_nb15_train.csv"
OUT_TEST = OUT_DIR / "unsw_nb15" / "unsw_nb15_test.csv"

TRAIN_SIZE = 20000
TEST_SIZE = 4000
RANDOM_STATE = 42
TARGET_COL = "label"

IS_MULTI_CLASS = False
IS_REGRESSION = False

TRAIN_POS_RATIO = None
TEST_POS_RATIO = None

CATEGORICAL_COLS = ["state", "proto", "service", "ocean_proximity"]

HAS_DRIFT = False
TIME_COL = None

ID_CANDIDATES = ["__id", "id", "ID", "Id", "key", "Key"]


def load_df(path: Path) -> pd.DataFrame:
    p = Path(path)
    ext = p.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(p)
    if ext == ".parquet":
        return pd.read_parquet(p)
    raise ValueError(f"Unsupported input format: {ext} (expected .csv or .parquet)")


def ensure_global_id(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    for col in ID_CANDIDATES:
        if col in df.columns:
            out = df.copy()
            if col != "__id":
                out = out.rename(columns={col: "__id"})
            out["__id"] = out["__id"].astype("int64")
            return out, "__id"

    out = df.copy()
    out["__id"] = np.arange(len(out), dtype=np.int64)
    return out, "__id"


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
            raise ValueError(f"Not enough rows for class {cls}. Need {n}, have {len(df_cls)}.")
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


def apply_label_encoders(df: pd.DataFrame, encoders: dict[str, dict[str, int]], categorical_cols: list[str]) -> pd.DataFrame:
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


def sample_binary_df(df: pd.DataFrame, size: int, target_col: str, pos_ratio: float | None, random_state: int) -> pd.DataFrame:
    raw_counts = df[target_col].value_counts().to_dict()
    raw_pos_ratio = raw_counts.get(1, 0) / len(df)
    ratio = raw_pos_ratio if pos_ratio is None else pos_ratio

    n0, n1 = class_counts_from_ratio(size, ratio)

    df_1 = df[df[target_col] == 1]
    df_0 = df[df[target_col] == 0]

    if len(df_1) < n1 or len(df_0) < n0:
        raise ValueError(
            f"Not enough rows per class. Need {{0:{n0}, 1:{n1}}}, have {{0:{len(df_0)}, 1:{len(df_1)}}}."
        )

    part_1 = df_1.sample(n=n1, random_state=random_state)
    part_0 = df_0.sample(n=n0, random_state=random_state)
    return pd.concat([part_0, part_1], axis=0).sample(frac=1.0, random_state=random_state)


def sample_df(df: pd.DataFrame, size: int, random_state: int, pos_ratio=None) -> pd.DataFrame:
    if IS_REGRESSION:
        return random_sample(df=df, size=size, random_state=random_state)

    if IS_MULTI_CLASS:
        return stratified_sample_multiclass(df=df, size=size, target_col=TARGET_COL, random_state=random_state)

    return sample_binary_df(df=df, size=size, target_col=TARGET_COL, pos_ratio=pos_ratio, random_state=random_state)


def split_without_drift(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = sample_df(df=df, size=TRAIN_SIZE, random_state=RANDOM_STATE, pos_ratio=TRAIN_POS_RATIO)

    train_idx = train.index
    eligible = df.drop(index=train_idx)

    print("\nEligible for test rows:", len(eligible))

    if len(eligible) < TEST_SIZE:
        raise ValueError(f"Not enough eligible rows for test. Need {TEST_SIZE}, have {len(eligible)}.")

    test = sample_df(df=eligible, size=TEST_SIZE, random_state=RANDOM_STATE, pos_ratio=TEST_POS_RATIO)

    overlap = len(set(train["__id"]) & set(test["__id"]))
    print("\nTrain ∩ Test (by __id):", overlap)

    return train.reset_index(drop=True), test.reset_index(drop=True)


def split_with_drift(df: pd.DataFrame, train_frac: float = 0.8, test_frac: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not (0.0 < train_frac < 1.0):
        raise ValueError(f"train_frac must be in (0,1), got {train_frac}")
    if not (0.0 < test_frac < 1.0):
        raise ValueError(f"test_frac must be in (0,1), got {test_frac}")
    if not np.isclose(train_frac + test_frac, 1.0):
        raise ValueError(f"train_frac + test_frac must be 1.0, got {train_frac + test_frac}")

    if TIME_COL is None:
        ordered = df.reset_index(drop=True).copy()
        print("\nHAS_DRIFT=True and TIME_COL=None -> preserving original row order.")
    else:
        if TIME_COL not in df.columns:
            raise ValueError(f"TIME_COL='{TIME_COL}' not found in dataset.")
        ordered = df.sort_values(TIME_COL, kind="stable").reset_index(drop=True).copy()
        print(f"\nHAS_DRIFT=True -> ordering by time column '{TIME_COL}'.")

    n = len(ordered)
    train_end = int(round(n * train_frac))
    train_end = max(1, min(train_end, n - 1))

    train = ordered.iloc[:train_end].copy().reset_index(drop=True)
    test = ordered.iloc[train_end:].copy().reset_index(drop=True)

    overlap = len(set(train["__id"]) & set(test["__id"]))
    print("\nTrain ∩ Test (by __id):", overlap)

    print("\nDrift split:")
    print("Total rows:", n)
    print("Train rows:", len(train))
    print("Test rows :", len(test))

    return train, test


IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_TEST.parent.mkdir(parents=True, exist_ok=True)

if IS_MULTI_CLASS and IS_REGRESSION:
    raise ValueError("IS_MULTI_CLASS and IS_REGRESSION cannot both be True.")

if not IS_MULTI_CLASS and not IS_REGRESSION:
    validate_ratio("TRAIN_POS_RATIO", TRAIN_POS_RATIO)
    validate_ratio("TEST_POS_RATIO", TEST_POS_RATIO)

df = load_df(RAW_PATH)
df, id_col = ensure_global_id(df)

print("RAW rows:", len(df))
print("RAW cols:", df.shape[1])
print("Using global id column:", id_col)
print_balance("RAW", df)
print_regression_stats("RAW", df)

if HAS_DRIFT:
    train, test = split_with_drift(df)
else:
    train, test = split_without_drift(df)

print("\nTrain rows:", len(train))
print_balance("TRAIN", train)
print_regression_stats("TRAIN", train)

print("\nTest rows:", len(test))
print_balance("TEST", test)
print_regression_stats("TEST", test)

encoders = build_label_encoders(train, CATEGORICAL_COLS)
print_encoder_summary(encoders)

train = apply_label_encoders(train, encoders, CATEGORICAL_COLS)
test = apply_label_encoders(test, encoders, CATEGORICAL_COLS)

train.to_csv(OUT_TRAIN, index=False)
test.to_csv(OUT_TEST, index=False)

print("Encoded categorical columns:", [c for c in CATEGORICAL_COLS if c in train.columns])
print("Final train cols:", train.shape[1])
print("Final test cols:", test.shape[1])
print("Train has __id:", "__id" in train.columns)
print("Test has __id:", "__id" in test.columns)
print("Done.")