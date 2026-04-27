"""
Data Validation - Version 1: Pure Pandas (No External Libraries)
================================================================
Applied Data Science - Cairo University
This version uses only pandas to validate data.
Recommended for: Beginners, teaching core concepts.
"""

import pandas as pd
import numpy as np
from datetime import datetime


class DataValidator:
    """
    Simple data validator using only pandas.
    Easy to understand, no extra installations needed.
    """

    def __init__(self):
        self.validation_results = []

    def validate_schema(self, df, expected_columns, expected_types):
        """Check column presence and data types."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Schema',
            'passed': True,
            'issues': [],
        }

        # Check for missing columns
        missing_cols = set(expected_columns) - set(df.columns)
        if missing_cols:
            report['passed'] = False
            report['issues'].append(f"Missing columns: {missing_cols}")

        # Check for unexpected extra columns
        extra_cols = set(df.columns) - set(expected_columns)
        if extra_cols:
            report['issues'].append(f"Extra columns (not in schema): {extra_cols}")

        # Check data types match expected
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

    def validate_ranges(self, df, range_rules):
        """Check that numeric values fall within expected min/max bounds."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Ranges',
            'passed': True,
            'issues': [],
        }

        for col, rules in range_rules.items():
            if col not in df.columns:
                continue

            if 'min' in rules:
                below_min = df[df[col] < rules['min']]
                if len(below_min) > 0:
                    report['passed'] = False
                    report['issues'].append(
                        f"Column '{col}': {len(below_min)} values below minimum ({rules['min']})"
                    )

            if 'max' in rules:
                above_max = df[df[col] > rules['max']]
                if len(above_max) > 0:
                    report['passed'] = False
                    report['issues'].append(
                        f"Column '{col}': {len(above_max)} values above maximum ({rules['max']})"
                    )

        self.validation_results.append(report)
        return report

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

            duplicate_count = df[col].duplicated().sum()
            if duplicate_count > 0:
                report['passed'] = False
                report['issues'].append(
                    f"Column '{col}': {duplicate_count} duplicate values found"
                )

        self.validation_results.append(report)
        return report

    def validate_categorical(self, df, categorical_rules):
        """Check that categorical columns only contain allowed values."""
        report = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'check_type': 'Categorical',
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

    def generate_report(self):
        """Print a clear, readable validation summary report."""
        total = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r['passed'])
        failed = total - passed
        success_rate = (passed / total * 100) if total > 0 else 0

        print("=" * 55)
        print("       DATA VALIDATION REPORT")
        print("=" * 55)
        print(f"  Total Checks  : {total}")
        print(f"  Passed        : {passed}")
        print(f"  Failed        : {failed}")
        print(f"  Success Rate  : {success_rate:.1f}%")
        print("=" * 55)

        for result in self.validation_results:
            status = "PASS" if result['passed'] else "FAIL"
            print(f"\n[{status}] {result['check_type']} Check")
            print(f"   Time: {result['timestamp']}")

            if result['issues']:
                for issue in result['issues']:
                    print(f"   {issue}")
            else:
                print("   No issues found.")

        print("\n" + "=" * 55)

        return {
            'total': total,
            'passed': passed,
            'failed': failed,
            'success_rate': success_rate,
            'details': self.validation_results,
        }


# ─────────────────────────────────────────────
# GENERATE SYNTHETIC DATA (same as before)
# ─────────────────────────────────────────────
np.random.seed(42)
n = 200

df = pd.DataFrame(
    {
        'user_id': range(1, n + 1),
        'name': ['User_' + str(i) for i in range(1, n + 1)],
        'email': ['user' + str(i) + '@email.com' for i in range(1, n + 1)],
        'age': np.random.randint(18, 80, n),
        'signup_date': pd.date_range(start='2022-01-01', periods=n, freq='D'),
        'country': np.random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany'], n),
    }
)

# Inject intentional errors
df.loc[5, 'email'] = None  # missing email
df.loc[10, 'age'] = -5  # invalid age (below 0)
df.loc[15, 'country'] = 'Egypt'  # invalid country
df.loc[1, 'user_id'] = 1  # duplicate user_id

# Save and reload (this converts datetime to string — common real-world scenario)
df.to_csv('users.csv', index=False)
df = pd.read_csv('users.csv', parse_dates=['signup_date'])  # fix: parse dates on load

# ─────────────────────────────────────────────
# DEFINE RULES
# ─────────────────────────────────────────────
expected_columns = ['user_id', 'name', 'email', 'age', 'signup_date', 'country']

expected_types = {
    'user_id': 'int64',
    'name': 'str',
    'email': 'str',
    'age': 'int64',
    'signup_date': 'datetime64[us]',
    'country': 'str',
}

# NOTE:
# The New Behavior (pandas 2.0+ / 3.0+)
# Newer pandas introduced a proper dedicated string dtype called StringDtype. When pandas now infers column types, it recognizes string columns explicitly:
# python# New pandas
# df['name'].dtype    # → StringDtype()
# str(df['name'].dtype)  # → 'str'   ← no longer 'object'

range_rules = {'age': {'min': 0, 'max': 120}, 'user_id': {'min': 1}}

categorical_rules = {'country': ['USA', 'UK', 'Canada', 'Australia', 'Germany']}

# ─────────────────────────────────────────────
# RUN VALIDATION
# ─────────────────────────────────────────────
validator = DataValidator()

validator.validate_schema(df, expected_columns, expected_types)
validator.validate_completeness(
    df, required_columns=['user_id', 'email'], max_missing_pct=0.01
)
validator.validate_ranges(df, range_rules)
validator.validate_uniqueness(df, unique_columns=['user_id', 'email'])
validator.validate_categorical(df, categorical_rules)

validator.generate_report()
