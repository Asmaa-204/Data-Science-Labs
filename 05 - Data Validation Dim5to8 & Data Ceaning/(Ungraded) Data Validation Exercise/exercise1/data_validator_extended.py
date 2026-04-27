"""
Data Validation - Extended DataValidator (Dimensions 1–8)
==========================================================

This file is the STARTER FILE provided to students.
It contains:
  - The original DataValidator class (Dimensions 1–4) → keep as-is
  - Empty stub functions for Dimensions 5–8 → students must implement

Datasets for testing:
  - datasets/ecommerce_orders.csv
  - datasets/patient_health_records.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime
from scipy import stats
from scipy.stats import zscore, kstest, skew, kurtosis
from sklearn.ensemble import IsolationForest


# ══════════════════════════════════════════════════════════════════════════════
# ORIGINAL DataValidator (Dimensions 1–4) — DO NOT MODIFY
# ══════════════════════════════════════════════════════════════════════════════


class DataValidator:
    """
    Generic data validator using only standard Python libraries.
    Extended to cover all 8 validation dimensions from the lecture.
    """

    def __init__(self):
        self.validation_results = []

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 1 – ACCURACY
    # ──────────────────────────────────────────────────────────────────────────
    def validate_schema(self, df, expected_columns, expected_types):
        """Check column presence and data types."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Schema (Accuracy)',
            'passed': True,
            'issues': [],
        }

        missing_cols = set(expected_columns) - set(df.columns)
        if missing_cols:
            report['passed'] = False
            report['issues'].append(f"Missing columns: {missing_cols}")

        extra_cols = set(df.columns) - set(expected_columns)
        if extra_cols:
            report['issues'].append(f"Extra columns (not in schema): {extra_cols}")

        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    report['passed'] = False
                    report['issues'].append(
                        f"Column '{col}': expected type '{expected_type}', got '{actual_type}'"
                    )

        self.validation_results.append(report)
        return report

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 3 – COMPLETENESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_completeness(self, df, required_columns, max_missing_pct=0.05):
        """Check that required columns don't have too many missing values."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Completeness',
            'passed': True,
            'issues': [],
        }

        for col in required_columns:
            if col not in df.columns:
                report['passed'] = False
                report['issues'].append(f"Required column '{col}' not found in data")
                continue

            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / len(df)) * 100

            if missing_pct > max_missing_pct * 100:
                report['passed'] = False
                report['issues'].append(
                    f"Column '{col}': {missing_pct:.2f}% missing "
                    f"(allowed max: {max_missing_pct * 100:.0f}%)"
                )

        self.validation_results.append(report)
        return report

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 1 – ACCURACY (business rule: range check)
    # ──────────────────────────────────────────────────────────────────────────
    def validate_ranges(self, df, range_rules):
        """Check that numeric values fall within expected min/max bounds."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Ranges (Accuracy)',
            'passed': True,
            'issues': [],
        }

        for col, rules in range_rules.items():
            if col not in df.columns:
                continue

            col_data = df[col].dropna()

            if 'min' in rules:
                below_min = col_data[col_data < rules['min']]
                if len(below_min) > 0:
                    report['passed'] = False
                    report['issues'].append(
                        f"Column '{col}': {len(below_min)} values below minimum ({rules['min']})"
                    )

            if 'max' in rules:
                above_max = col_data[col_data > rules['max']]
                if len(above_max) > 0:
                    report['passed'] = False
                    report['issues'].append(
                        f"Column '{col}': {len(above_max)} values above maximum ({rules['max']})"
                    )

        self.validation_results.append(report)
        return report

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 4 – UNIQUENESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_uniqueness(self, df, unique_columns):
        """Check that key columns have no duplicate values."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Uniqueness',
            'passed': True,
            'issues': [],
        }

        for col in unique_columns:
            if col not in df.columns:
                continue

            duplicate_count = df[col].dropna().duplicated().sum()
            if duplicate_count > 0:
                report['passed'] = False
                report['issues'].append(
                    f"Column '{col}': {duplicate_count} duplicate values found"
                )

        self.validation_results.append(report)
        return report

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 2 – CONSISTENCY (categorical allowed values)
    # ──────────────────────────────────────────────────────────────────────────
    def validate_categorical(self, df, categorical_rules):
        """Check that categorical columns only contain allowed values."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Categorical (Consistency)',
            'passed': True,
            'issues': [],
        }

        for col, allowed_values in categorical_rules.items():
            if col not in df.columns:
                continue

            actual_values = set(df[col].dropna().unique())
            invalid_values = actual_values - set(allowed_values)

            if invalid_values:
                report['passed'] = False
                report['issues'].append(
                    f"Column '{col}': invalid values found: {invalid_values}. "
                    f"Allowed: {set(allowed_values)}"
                )

        self.validation_results.append(report)
        return report

    # ══════════════════════════════════════════════════════════════════════════
    # ▼▼▼  STUDENTS: IMPLEMENT THE FOUR FUNCTIONS BELOW  ▼▼▼
    # ══════════════════════════════════════════════════════════════════════════

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 5 – OUTLIERS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim5_outliers(
        self,
        df,
        numeric_columns,
        zscore_threshold=3.0,
        iqr_multiplier=1.5,
        isolation_contamination=0.05,
    ):
        """
        Detect outliers using THREE methods:
          1. Z-Score  (assumes normal distribution)
          2. IQR      (Inter-Quartile Range, robust for skewed data)
          3. Isolation Forest (multivariate, best for complex datasets)

        Parameters
        ----------
        df : pd.DataFrame
        numeric_columns : list[str]
            Columns to check for outliers (must be numeric).
        zscore_threshold : float
            Absolute z-score above which a value is flagged (default 3).
        iqr_multiplier : float
            Multiplier for IQR bounds (default 1.5).
        isolation_contamination : float
            Expected proportion of outliers for Isolation Forest (default 0.05).

        Returns
        -------
        dict  validation report
        """
        # TODO: implement this function
        # Hint: use scipy.stats.zscore, IQR formulas, and
        # sklearn.ensemble.IsolationForest
        # For each column, report per-method outlier counts.
        # For Isolation Forest, use ALL numeric_columns together.
        raise NotImplementedError("validate_dim5_outliers() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 6 – TIMELINESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim6_timeliness(
        self,
        df,
        timestamp_column,
        max_age_hours=None,
        expected_frequency=None,
        expected_records_per_period=None,
        period_hours=24,
    ):
        """
        Validate time-based data properties:
          1. Data freshness  – how old is the most recent record?
          2. Chronological order – are timestamps monotonically increasing?
          3. Expected volume  – does record count per period match expectations?
          4. Gap detection    – are there unexpected gaps in the time series?

        Parameters
        ----------
        df : pd.DataFrame
        timestamp_column : str
            Name of the datetime column.
        max_age_hours : float or None
            If set, warn if most-recent record is older than this many hours.
        expected_frequency : str or None
            Pandas offset alias for expected frequency, e.g. 'D', 'H', '10min'.
            If set, check for gaps/duplicates in the time series index.
        expected_records_per_period : int or None
            Expected record count within the last `period_hours`. Warn if
            actual count < 80% of this value.
        period_hours : int
            Look-back window in hours for volume check (default 24).

        Returns
        -------
        dict  validation report
        """
        # TODO: implement this function
        raise NotImplementedError("validate_dim6_timeliness() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 7 – DISTRIBUTION PROFILE
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim7_distribution(
        self,
        df,
        numeric_columns,
        reference_df=None,
        ks_pvalue_threshold=0.05,
        skewness_threshold=1.0,
        kurtosis_threshold=3.0,
    ):
        """
        Examine how values are distributed within fields:
          1. Descriptive statistics (min, max, mean, median, std, skewness,
             kurtosis, quartiles).
          2. Skewness check – flag columns with |skew| > skewness_threshold.
          3. Kurtosis check – flag columns with |excess kurtosis| > kurtosis_threshold.
          4. KS test (normality) against a theoretical normal distribution.
          5. KS two-sample test vs. a reference DataFrame (if provided), useful
             for detecting distribution drift between training and production data.

        Parameters
        ----------
        df : pd.DataFrame
        numeric_columns : list[str]
        reference_df : pd.DataFrame or None
            Baseline / training dataset to compare distributions against.
        ks_pvalue_threshold : float
            p-value below which the KS test is considered significant (default 0.05).
        skewness_threshold : float
            |skewness| above which a column is flagged (default 1.0).
        kurtosis_threshold : float
            |excess kurtosis| above which a column is flagged (default 3.0).

        Returns
        -------
        dict  validation report
        """
        # TODO: implement this function
        # Hint: use scipy.stats.skew, kurtosis, kstest, ks_2samp
        raise NotImplementedError("validate_dim7_distribution() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 8 – RELATIONSHIPS / CORRELATION
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim8_relationships_correlation(
        self,
        df,
        numeric_columns,
        high_corr_threshold=0.9,
        expected_correlations=None,
        unexpected_correlations=None,
    ):
        """
        Examine correlations and dependencies between fields:
          1. Pearson correlation matrix  (linear, assumes normality).
          2. Spearman correlation matrix (monotonic, robust to outliers).
          3. Comparison between Pearson and Spearman to flag non-linear
             relationships (large |Pearson - Spearman| > 0.1).
          4. High-correlation detection – flag pairs above high_corr_threshold.
          5. Expected-correlation check – verify that certain pairs should be
             correlated (e.g. height & weight).
          6. Unexpected-correlation check – verify that certain pairs should NOT
             be correlated (e.g. customer_id & purchase_amount).

        Parameters
        ----------
        df : pd.DataFrame
        numeric_columns : list[str]
        high_corr_threshold : float
            Absolute correlation above which a pair is flagged (default 0.9).
        expected_correlations : list[tuple] or None
            List of (col_a, col_b, min_expected_r) tuples.
            Example: [('height_cm', 'weight_kg', 0.3)]
        unexpected_correlations : list[tuple] or None
            List of (col_a, col_b, max_allowed_r) tuples.
            Example: [('patient_id', 'bp_systolic', 0.1)]

        Returns
        -------
        dict  validation report
        """
        # TODO: implement this function
        # Hint: use df.corr(method='pearson') and df.corr(method='spearman')
        raise NotImplementedError(
            "validate_dim8_relationships_correlation() not yet implemented"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # REPORT GENERATOR (original – keep as-is, you may extend the print format)
    # ══════════════════════════════════════════════════════════════════════════
    def generate_report(self):
        """Print a clear, readable validation summary report."""
        total = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r['passed'])
        failed = total - passed
        success_rate = (passed / total * 100) if total > 0 else 0

        print("=" * 60)
        print("         DATA VALIDATION REPORT")
        print("=" * 60)
        print(f"  Total Checks  : {total}")
        print(f"  Passed        : {passed}")
        print(f"  Failed        : {failed}")
        print(f"  Success Rate  : {success_rate:.1f}%")
        print("=" * 60)

        for result in self.validation_results:
            status = "PASS" if result['passed'] else "FAIL"
            print(f"\n[{status}] {result['check_type']} Check")
            print(f"   Time: {result['timestamp']}")

            if result['issues']:
                for issue in result['issues']:
                    print(f"   ⚠  {issue}")
            else:
                print("   ✓  No issues found.")

        print("\n" + "=" * 60)

        return {
            'total': total,
            'passed': passed,
            'failed': failed,
            'success_rate': success_rate,
            'details': self.validation_results,
        }


# ══════════════════════════════════════════════════════════════════════════════
# TESTING SECTION  (students must complete this section)
# ══════════════════════════════════════════════════════════════════════════════


def validate_ecommerce_dataset():
    """Validates the ecommerce dataset."""

    # ── Validate E-Commerce Orders ────────────────────────────────────────────
    print("\n" + "█" * 60)
    print("  DATASET 1 – E-Commerce Orders")
    print("█" * 60)

    v1 = DataValidator()

    # Dimensions 1-4 (original)
    v1.validate_schema(
        orders,
        expected_columns=[
            'order_id',
            'customer_id',
            'order_timestamp',
            'category',
            'quantity',
            'unit_price',
            'discount_pct',
            'status',
            'country',
            'rating',
        ],
        expected_types={
            'order_id': 'int64',
            'unit_price': 'float64',
            'quantity': 'int64',
        },
    )
    v1.validate_completeness(
        orders,
        required_columns=['order_id', 'customer_id', 'unit_price', 'status'],
        max_missing_pct=0.02,
    )
    v1.validate_ranges(
        orders,
        {
            'unit_price': {'min': 0},
            'discount_pct': {'min': 0, 'max': 1},
            'rating': {'min': 1, 'max': 5},
            'quantity': {'min': 1},
        },
    )
    v1.validate_uniqueness(orders, unique_columns=['order_id'])
    v1.validate_categorical(
        orders,
        {
            'status': ['pending', 'shipped', 'delivered', 'cancelled', 'returned'],
            'country': ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia'],
        },
    )

    # TODO: call Dimensions 5-8 here
    numeric_cols_orders = ['quantity', 'unit_price', 'discount_pct', 'rating']

    # v1.validate_dim5_outliers(orders, numeric_cols_orders)
    # v1.validate_dim6_timeliness(orders, timestamp_column='order_timestamp', max_age_hours=8760)
    # v1.validate_dim7_distribution(orders, numeric_cols_orders)
    # v1.validate_dim8_relationships_correlation(orders, numeric_cols_orders,
    #     expected_correlations=[('unit_price', 'rating', 0.0)],
    #     unexpected_correlations=[('order_id', 'unit_price', 0.1)])

    v1.generate_report()


def validate_patients_dataset():
    """Validates the patients dataset."""

    # ── Validate Patient Health Records ───────────────────────────────────────
    print("\n" + "█" * 60)
    print("  DATASET 2 – Patient Health Records")
    print("█" * 60)

    v2 = DataValidator()

    v2.validate_schema(
        health,
        expected_columns=[
            'patient_id',
            'age',
            'gender',
            'blood_type',
            'admission_date',
            'department',
            'height_cm',
            'weight_kg',
            'bp_systolic',
            'bp_diastolic',
        ],
        expected_types={'patient_id': 'int64', 'age': 'int64'},
    )
    v2.validate_completeness(
        health,
        required_columns=['patient_id', 'age', 'bp_systolic'],
        max_missing_pct=0.02,
    )
    v2.validate_ranges(
        health,
        {
            'age': {'min': 0, 'max': 120},
            'height_cm': {'min': 50, 'max': 250},
            'weight_kg': {'min': 1, 'max': 300},
            'bp_systolic': {'min': 60, 'max': 250},
            'bp_diastolic': {'min': 40, 'max': 150},
        },
    )
    v2.validate_uniqueness(health, unique_columns=['patient_id'])
    v2.validate_categorical(
        health,
        {
            'gender': ['Male', 'Female'],
            'blood_type': ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'],
            'department': [
                'Cardiology',
                'Neurology',
                'Orthopedics',
                'Oncology',
                'Pediatrics',
            ],
        },
    )

    # TODO: call Dimensions 5-8 here
    numeric_cols_health = [
        'age',
        'height_cm',
        'weight_kg',
        'bp_systolic',
        'bp_diastolic',
    ]

    # v2.validate_dim5_outliers(health, numeric_cols_health)
    # v2.validate_dim6_timeliness(health, timestamp_column='admission_date', max_age_hours=17520)
    # v2.validate_dim7_distribution(health, numeric_cols_health)
    # v2.validate_dim8_relationships_correlation(health, numeric_cols_health,
    #     expected_correlations=[('height_cm', 'weight_kg', 0.3)],
    #     unexpected_correlations=[('patient_id', 'age', 0.1)])

    v2.generate_report()


if __name__ == "__main__":

    # ── Load datasets ─────────────────────────────────────────────────────────
    orders = pd.read_csv(
        '../datasets/ecommerce_orders.csv', parse_dates=['order_timestamp']
    )
    health = pd.read_csv(
        '../datasets/patient_health_records.csv', parse_dates=['admission_date']
    )

    # ── Validate E-Commerce Orders ────────────────────────────────────────────
    validate_ecommerce_dataset()

    # ── Validate Patient Health Records ───────────────────────────────────────
    validate_patients_dataset()
