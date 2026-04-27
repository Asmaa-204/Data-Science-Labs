# Assignment 03 – Data Validation (Dimensions 5–8 & Great Expectations)
**Applied Data Science – Cairo University, Faculty of Engineering**
**Spring 2026 | Instructor: Dr. Salma Abdelmonem**

---

## Overview

This assignment builds directly on **Lecture 03 – Data Validation**.
You will extend a working Python data-validation framework to cover all **8 quality dimensions** from the lecture, and then re-implement the same framework using the industry-standard **Great Expectations (GX) v1.x** library.


**Total grade:** 100 points


---

## Learning Objectives

By completing this assignment you will be able to:

- Implement statistical outlier detection (Z-Score, IQR, Isolation Forest).
- Validate time-series data for freshness, ordering, and expected volume.
- Profile and compare data distributions using skewness, kurtosis, and the KS test.
- Detect problematic correlations using Pearson and Spearman methods.
- Use the Great Expectations v1.x API to build automated, documented validation pipelines.

---

## Provided Files

```
assignment/
├── datasets/
│   ├── ecommerce_orders.csv          ← Dataset 1 (3 000 rows, 10 cols)
│   └── patient_health_records.csv    ← Dataset 2 (2 000 rows, 10 cols)
│
├── exercise1/
│   └── data_validator_extended.py    ← Starter file (Exercise 1)
│
├── exercise2/
│   └── gx_data_validator.py          ← Starter file (Exercise 2)
│
└── ASSIGNMENT.md                     ← This file
```

### Dataset Descriptions

**Dataset 1 – `ecommerce_orders.csv`**
Online store order records with columns: `order_id`, `customer_id`, `order_timestamp`, `category`, `quantity`, `unit_price`, `discount_pct`, `status`, `country`, `rating`.

**Dataset 2 – `patient_health_records.csv`**
Hospital patient records with columns: `patient_id`, `age`, `gender`, `blood_type`, `admission_date`, `department`, `height_cm`, `weight_kg`, `bp_systolic`, `bp_diastolic`.

Both datasets contain **intentionally injected errors** that your validator must detect.

---

## Required Folder Structure (Submission)

Your submission ZIP must follow this structure exactly:

```
StudentName_ID_Assignment03/
│
├── exercise1/
│   ├── data_validator_extended.py    ← Your implemented class (Dims 1–8)
│   └── README_exercise1.md           ← Required readme (see spec below)
│
├── exercise2/
│   ├── gx_data_validator.py          ← Your GX class implementation
│   ├── html_reports/
│   │   ├── ecommerce_gx_report.html  ← GX Data Docs for Dataset 1
│   │   └── health_gx_report.html     ← GX Data Docs for Dataset 2
│   └── README_exercise2.md           ← Required readme (see spec below)
│
└── datasets/
    ├── ecommerce_orders.csv
    └── patient_health_records.csv
```

---

## Exercise 1 – Extend `DataValidator` (60 points)

### Task Description

Open `exercise1/data_validator_extended.py`. The file already contains the original `DataValidator` class covering Dimensions 1–4. Your task is to implement the **four stub functions** for Dimensions 5–8.

---

### Before the New Functions

- For the report dictionary returned by each **validate_** function the existing or the new, add new `details` key for report details, like counts, percentages, etc.

- Modify the existing **validate_** functions to put the appropriate value of the `details` key.

- Maintain the other keys (`passed`, `issues`, etc.) of the dict with their meaning.


- You can add more keys to the `report` dict as needed, but make sure to also maintain the meaning of the existing keys.


### New Function Specifications



- For all the following function, decide the **default value of each parameter**. Feel free to add more parameters but with maintaining generality.

#### `validate_dim5_outliers(df, numeric_columns, zscore_threshold, iqr_multiplier, isolation_contamination)`

Implement **all three** outlier detection methods from the lecture:

1. **Z-Score method** – flag values with `|z| > zscore_threshold` per column.
2. **IQR method** – compute `Q1`, `Q3`, `IQR = Q3 - Q1`; flag values outside `[Q1 - iqr_multiplier*IQR, Q3 + iqr_multiplier*IQR]` per column.
3. **Isolation Forest** – use all `numeric_columns` together; flag rows where `predict() == -1`.

For each method, report the outlier count and a sample of flagged values. For Isolation Forest, report the total number of anomalous rows detected.

The report `passed` field should be `False` if any method flags outliers in any column.

---

#### `validate_dim6_timeliness(df, timestamp_column, max_age_hours, expected_frequency, expected_records_per_period, period_hours)`

Implement **all four** timeliness checks from the lecture:

1. **Freshness** – compute `(now - max(timestamp)).total_seconds() / 3600`; warn if `> max_age_hours`.
2. **Chronological order** – check `is_monotonic_increasing` on the sorted timestamp column.
3. **Volume check** – count records in the last `period_hours`; warn if count `< 0.8 * expected_records_per_period`.
4. **Gap detection** – if `expected_frequency` is provided, use `pd.date_range` to check for missing periods in the time series.

---

#### `validate_dim7_distribution(df, numeric_columns, reference_df, ks_pvalue_threshold, skewness_threshold, kurtosis_threshold)`

Implement **all five** distribution checks:

1. **Descriptive statistics** – for each column compute min, max, mean, median, mode, std, Q1, Q3, skewness, kurtosis and include in the report.
2. **Skewness check** – flag columns where `|skewness| > skewness_threshold`.
3. **Kurtosis check** – flag columns where `|excess kurtosis| > kurtosis_threshold`.
4. **KS normality test** – run `scipy.stats.kstest(col, 'norm', args=(mean, std))`; flag if `p_value < ks_pvalue_threshold`.
5. **KS two-sample drift test** – if `reference_df` is provided, run `scipy.stats.ks_2samp` between the reference and current column; flag if `p_value < ks_pvalue_threshold` (indicates distribution drift).

**NOTE:** Tests whether two independent samples were drawn from the same continuous distribution. Unlike `kstest()` (which compares a sample to a *theoretical* distribution), `ks_2samp` compares two real datasets against each other — no assumed distribution needed.

---

#### `validate_dim8_relationships_correlation(df, numeric_columns, high_corr_threshold, expected_correlations, unexpected_correlations)`

Implement **all four** correlation checks:

1. **Pearson correlation matrix** – compute and include in report.
2. **Spearman correlation matrix** – compute and include in report.
3. **Non-linearity detection** – for each pair, if `|pearson_r - spearman_r| > 0.1`, flag the pair as potentially non-linear.
4. **High-correlation detection** – flag any pair with `|pearson_r| > high_corr_threshold`.
5. **Expected-correlation check** – if `expected_correlations` is provided, warn if the observed `|r| < min_expected_r`.
6. **Unexpected-correlation check** – if `unexpected_correlations` is provided, warn if the observed `|r| > max_allowed_r`.

---

### Generic Design Requirements

- All four functions must work on **any pandas DataFrame**, not just the provided datasets.
- Parameters must have sensible default values so the functions can be called with only `df` and the column list.
- Do **not** hardcode column names or dataset-specific thresholds inside the functions.

### Optional

Add a `validate_dim1_accuracy()`, `validate_dim2_consistency()`, `validate_dim3_completeness()`, and `validate_dim4_uniqueness()` wrapper that call the original functions internally and follow the same interface pattern as Dims 5–8.

### Testing Requirements

In the `if __name__ == "__main__":` block, run the full validator on **both** datasets (put validation calls of each dataset in a global function and call them here). Print the complete validation reports.

---

### README Requirements – `README_exercise1.md`

Your readme must contain the following sections:

1. **How to Run** – exact shell commands to install dependencies and execute the script.
2. **Report Explanation** – describe what each validation dimension reports and how to interpret PASS/FAIL.
3. **Dataset 1 Findings** – the concrete observations about `ecommerce_orders.csv` from your validation results (e.g. which columns have outliers, whether the distribution is skewed, what correlations exist).
4. **Dataset 2 Findings** – the concrete observations about `patient_health_records.csv`.
5. **Screenshots** – include terminal screenshots of the printed validation reports.

---

## Exercise 2 – `GXDataValidator` using Great Expectations v1.x (35 points)

### Task Description

Open `exercise2/gx_data_validator.py`. Implement the `GXDataValidator` class so that it has **the same external interface** as `DataValidator` from Exercise 1, but uses the **Great Expectations v1.x** API internally.

### Requirements

#### Initialization (`setup_context`)

Implement `setup_context(df, ...)` to:
- Create an ephemeral GX context: `gx.get_context(mode="ephemeral")`.
- Add a Pandas data source and DataFrame asset.
- Add a batch definition for the whole DataFrame.
- Create and store an `ExpectationSuite`.

All GX objects must be stored as instance attributes for use by `validate_*()` methods.

#### `validate_schema`, `validate_completeness`, `validate_ranges`, `validate_uniqueness`, `validate_categorical`

Each method must append the appropriate GX expectation objects to `self.suite`. Refer to the docstrings for which GX expectation class to use for each method.

#### `validate_dim5_outliers`, `validate_dim6_timeliness`, `validate_dim7_distribution` `validate_dim8_relationships_correlation`

GX does not have native outlier or correlation expectations. Use the best available GX alternatives described in each docstring. Document which GX expectations you used and why in your README.

Also, self-study how to create a custom gx expectation.

Since GX lacks a native correlation expectation, you may use a combination of GX statistical expectations AND include a note in the suite description. Document your approach clearly.

#### `run_validation(df)` and `generate_report(results)`

- `run_validation` must create a `ValidationDefinition`, link it to the suite and batch, and run it. Return the raw results object.
- `generate_report` must print a human-readable summary similar to Exercise 1 and return a summary dict.

#### `build_and_open_docs()`

Call `context.build_data_docs()` and `context.open_data_docs()`. Save the generated HTML files to `html_reports/` and include them in your submission.

### Testing Requirements

Validate both datasets. Attach the two generated HTML Data Docs files (`ecommerce_gx_report.html`, `health_gx_report.html`) in your submission.

### README Requirements – `README_exercise2.md`

1. **Installation** – exact `pip install` commands.
2. **How to Run** – commands to execute the script and regenerate HTML reports.
3. **GX API Summary** – brief explanation of the GX v1.x workflow (context → data source → asset → batch → suite → validation definition → run).
4. **Expectation Mapping** – table showing which GX expectation you used for each validation dimension.
5. **Limitations** – which dimensions could not be fully replicated in GX and why.
6. **Dataset Findings** – same as Exercise 1: at least 5 observations per dataset.
7. **Screenshots** – at least 2 screenshots of the HTML Data Docs reports.

---

## Grading Rubric

| Component | Points |
|-----------|--------|
| **Exercise 1 – Implementation** | **50** |
| `validate_dim5_outliers()` – all 3 methods implemented correctly | 14 |
| `validate_dim6_timeliness()` – all 4 checks implemented correctly | 12 |
| `validate_dim7_distribution()` – all 5 checks (incl. KS tests) | 14 |
| `validate_dim8_relationships_correlation()` – all checks incl. Pearson & Spearman | 10 |
| **Exercise 1 – Usage & Quality** | **10** |
| Both datasets tested with full parameter sets | 4 |
| Generic design (no hardcoded values inside functions) | 3 |
| Code quality, comments, docstrings | 3 |
| **Exercise 1 – README** | **5** |
| How to run + report explanation + dataset findings + screenshots | 5 |
| **Exercise 2 – GX Implementation** | **25** |
| `setup_context()` correctly initialises GX context | 4 |
| `validate_schema/completeness/ranges/uniqueness/categorical` (5 × 2 pts) | 10 |
| `validate_dim5_outliers / dim6_timeliness / dim7_distribution / dim8_correlation` (4 × 2 pts) | 8 |
| `run_validation()` + `generate_report()` working end-to-end | 3 |
| **Exercise 2 – Docs & Quality** | **10** |
| HTML Data Docs generated and attached for both datasets | 4 |
| README: GX workflow, expectation mapping, limitations, findings, screenshots | 6 |
| **TOTAL** | **100** |

---

*Good luck! Remember: garbage in, garbage out — your model is only as good as your data.*
