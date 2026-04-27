"""
Data Validation - GX DataValidator using Great Expectations v1.x
=================================================================
This file is the STARTER FILE for Exercise 2.
Students must implement the GXDataValidator class below.

Install dependencies:
    pip install great-expectations>=1.13 pandas

Datasets for testing:
    - ../datasets/ecommerce_orders.csv
    - ../datasets/patient_health_records.csv
"""

import pandas as pd
import numpy as np


# ══════════════════════════════════════════════════════════════════════════════
# GXDataValidator  –  mirrors the interface of DataValidator (Exercise 1)
#                      but uses Great Expectations v1.x internally
# ══════════════════════════════════════════════════════════════════════════════


class GXDataValidator:
    """
    Data validator backed by Great Expectations v1.x (modern ephemeral API).

    Interface mirrors DataValidator from Exercise 1:
      - Each validate_*() method appends GX Expectations to the internal suite.
      - Call run_validation() to execute all expectations against the DataFrame.
      - Call generate_report() to print a readable summary.
      - Call build_and_open_docs() to produce and open the HTML Data Docs.

    Usage example
    -------------
    gxv = GXDataValidator(suite_name="orders_suite")
    gxv.setup_context(df, datasource_name="orders_ds")

    gxv.validate_schema(expected_columns=[...], expected_types={...})
    gxv.validate_completeness(required_columns=[...])
    gxv.validate_ranges(range_rules={...})
    gxv.validate_uniqueness(unique_columns=[...])
    gxv.validate_categorical(categorical_rules={...})
    gxv.validate_dim5_outliers(numeric_columns=[...])
    gxv.validate_dim6_timeliness(timestamp_column='...', ...)
    gxv.validate_dim7_istribution(numeric_columns=[...])
    gxv.validate_dim8_relationships_correlation(numeric_columns=[...])

    results = gxv.run_validation(df)
    gxv.generate_report(results)
    gxv.build_and_open_docs()
    """

    def __init__(self, suite_name: str = "default_suite"):
        """
        Parameters
        ----------
        suite_name : str
            Name of the GX ExpectationSuite to create.
        """
        self.suite_name = suite_name
        self.context = None  # gx.get_context(mode="ephemeral")
        self.suite = None  # gx.ExpectationSuite(...)
        self.batch_def = None  # data_asset.add_batch_definition_whole_dataframe(...)
        self.validation_def = None  # gx.ValidationDefinition(...)

    # ──────────────────────────────────────────────────────────────────────────
    # INITIALISATION HELPERS  (students must implement)
    # ──────────────────────────────────────────────────────────────────────────

    def setup_context(
        self,
        df: pd.DataFrame,
        datasource_name: str = "pandas_source",
        asset_name: str = "data_asset",
        batch_name: str = "full_batch",
    ):
        """
        Initialise the GX ephemeral context, connect it to `df`, and
        create an empty ExpectationSuite.

        Steps to implement
        ------------------
        1. gx.get_context(mode="ephemeral")
        2. context.data_sources.add_pandas(name=datasource_name)
        3. data_source.add_dataframe_asset(name=asset_name)
        4. data_asset.add_batch_definition_whole_dataframe(batch_name)
        5. context.suites.add(gx.ExpectationSuite(name=self.suite_name))

        Store all objects as instance attributes so the validate_*() methods
        can access self.suite to append expectations.
        """
        # TODO: implement
        raise NotImplementedError("setup_context() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 1 – ACCURACY / SCHEMA
    # ──────────────────────────────────────────────────────────────────────────
    def validate_schema(self, expected_columns: list, expected_types: dict = None):
        """
        Append schema expectations to the suite.

        GX expectations to use
        -----------------------
        - ExpectTableColumnsToMatchSet(column_set=expected_columns)
        - ExpectColumnValuesToBeOfType(column=col, type_=gx_type)
          for each entry in expected_types.

        Note: map Python/pandas dtype strings → GX type strings, e.g.
              'int64' → 'INTEGER', 'float64' → 'FLOAT', 'object' → 'STRING',
              'datetime64[ns]' → 'DATETIME'
        """
        # TODO: implement
        raise NotImplementedError("validate_schema() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 3 – COMPLETENESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_completeness(
        self, required_columns: list, max_missing_pct: float = 0.05
    ):
        """
        Append completeness expectations.

        GX expectation to use
        ---------------------
        - ExpectColumnValuesToNotBeNull(column=col,
              mostly=1.0 - max_missing_pct)
          for each column in required_columns.
        """
        # TODO: implement
        raise NotImplementedError("validate_completeness() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 1 – ACCURACY (business rule: value ranges)
    # ──────────────────────────────────────────────────────────────────────────
    def validate_ranges(self, range_rules: dict):
        """
        GX expectation to use
        ---------------------
        - ExpectColumnValuesToBeBetween(column=col,
              min_value=rules.get('min'), max_value=rules.get('max'))
        """
        # TODO: implement
        raise NotImplementedError("validate_ranges() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 4 – UNIQUENESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_uniqueness(self, unique_columns: list):
        """
        GX expectation to use
        ---------------------
        - ExpectColumnValuesToBeUnique(column=col)
        """
        # TODO: implement
        raise NotImplementedError("validate_uniqueness() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 2 – CONSISTENCY / CATEGORICAL
    # ──────────────────────────────────────────────────────────────────────────
    def validate_categorical(self, categorical_rules: dict):
        """
        GX expectation to use
        ---------------------
        - ExpectColumnValuesToBeInSet(column=col, value_set=allowed)
        """
        # TODO: implement
        raise NotImplementedError("validate_categorical() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 5 – OUTLIERS (GX approximations)
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim5_outliers(
        self,
        df: pd.DataFrame,
        numeric_columns: list,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
    ):
        """
        GX does not have a native IsolationForest expectation.
        Approximate outlier detection using:
          - ExpectColumnValuesToBeBetween with IQR-derived bounds.
          - ExpectColumnMeanToBeBetween / ExpectColumnStdevToBeBetween
            as distribution-health guards.

        Compute IQR bounds from `df` at setup time and pass them as
        min_value / max_value to ExpectColumnValuesToBeBetween.
        """
        # TODO: implement
        raise NotImplementedError("validate_dim5_outliers() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 6 – TIMELINESS
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim6_timeliness(
        self,
        timestamp_column: str,
        min_date: str = None,
        max_date: str = None,
        expected_row_count_min: int = None,
        expected_row_count_max: int = None,
    ):
        """
        GX expectations to use
        -----------------------
        - ExpectColumnValuesToBeBetween(column=timestamp_column,
              min_value=min_date, max_value=max_date)   ← date bounds
        - ExpectTableRowCountToBeBetween(
              min_value=expected_row_count_min,
              max_value=expected_row_count_max)
        """
        # TODO: implement
        raise NotImplementedError("validate_dim6_timeliness() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 7 – DISTRIBUTION PROFILE
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim7_istribution(
        self,
        df: pd.DataFrame,
        numeric_columns: list,
        mean_tolerance: float = 0.3,
        std_tolerance: float = 0.5,
    ):
        """
        GX expectations to use
        -----------------------
        - ExpectColumnMeanToBeBetween   – mean within ± mean_tolerance * mean
        - ExpectColumnStdevToBeBetween  – std dev within ± std_tolerance * std
        - ExpectColumnMedianToBeBetween – median within ± mean_tolerance * median
        - ExpectColumnQuantileValuesToBeBetween – check Q1 and Q3

        Compute baseline statistics from `df` at setup time.
        """
        # TODO: implement
        raise NotImplementedError("validate_dim7_istribution() not yet implemented")

    # ──────────────────────────────────────────────────────────────────────────
    # DIMENSION 8 – RELATIONSHIPS / CORRELATION
    # ──────────────────────────────────────────────────────────────────────────
    def validate_dim8_relationships_correlation(
        self, df: pd.DataFrame, column_pairs: list
    ):
        """
        GX does not have a native correlation expectation.
        Use ExpectColumnPairValuesToBeEqual or a custom expectation.

        Recommended approach: use
        ExpectColumnCorrelationToBeBetween (GX custom) if available,
        or implement using ExpectColumnPairValuesAToBeGreaterThanB
        as a proxy, or add a descriptive text note via
        ExpectTableRowCountToBeBetween as a placeholder while documenting
        the Pearson/Spearman results separately.

        Parameters
        ----------
        df : pd.DataFrame  (used to compute observed correlations for logging)
        column_pairs : list[tuple]
            Each tuple: (col_a, col_b, min_expected_r, max_expected_r)
        """
        # TODO: implement
        raise NotImplementedError(
            "validate_dim8_relationships_correlation() not yet implemented"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # RUN & REPORT
    # ──────────────────────────────────────────────────────────────────────────
    def run_validation(self, df: pd.DataFrame) -> object:
        """
        Create a ValidationDefinition, run it, and return the results object.

        Steps
        -----
        1. context.validation_definitions.add(
               gx.ValidationDefinition(name=..., data=self.batch_def,
                                       suite=self.suite))
        2. validation_def.run(batch_parameters={"dataframe": df})
        3. Return results.
        """
        # TODO: implement
        raise NotImplementedError("run_validation() not yet implemented")

    def generate_report(self, results) -> dict:
        """
        Print a readable summary of GX v1.x validation results.

        Iterate over results.results and print PASS/FAIL per expectation.
        Return a dict: {total, passed, failed, success_rate}.
        """
        # TODO: implement
        raise NotImplementedError("generate_report() not yet implemented")

    def build_and_open_docs(self):
        """
        Build HTML Data Docs and open them in the browser.
        Use: self.context.build_data_docs() and self.context.open_data_docs()
        """
        # TODO: implement
        raise NotImplementedError("build_and_open_docs() not yet implemented")


# ══════════════════════════════════════════════════════════════════════════════
# TESTING SECTION  (students must complete this section)
# ══════════════════════════════════════════════════════════════════════════════


def validate_ecommerce_dataset():
    """Validates the ecommerce dataset."""

    # ── E-Commerce Orders ─────────────────────────────────────────────────────
    print("\n" + "█" * 60)
    print("  DATASET 1 – E-Commerce Orders (GX)")
    print("█" * 60)

    gxv1 = GXDataValidator(suite_name="ecommerce_suite")
    # gxv1.setup_context(orders, datasource_name="ecommerce_ds")
    # gxv1.validate_schema(expected_columns=[...], expected_types={...})
    # gxv1.validate_completeness(required_columns=[...])
    # gxv1.validate_ranges(range_rules={...})
    # gxv1.validate_uniqueness(unique_columns=['order_id'])
    # gxv1.validate_categorical(categorical_rules={...})
    # gxv1.validate_dim5_outliers(orders, numeric_columns=[...])
    # gxv1.validate_dim6_timeliness(timestamp_column='order_timestamp', ...)
    # gxv1.validate_dim7_istribution(orders, numeric_columns=[...])
    # gxv1.validate_dim8_relationships_correlation(orders, column_pairs=[...])
    # results1 = gxv1.run_validation(orders)
    # gxv1.generate_report(results1)
    # gxv1.build_and_open_docs()

    # ── Patient Health Records ────────────────────────────────────────────────
    print("\n" + "█" * 60)
    print("  DATASET 2 – Patient Health Records (GX)")
    print("█" * 60)


def validate_patients_dataset():
    """Validates the patients dataset."""

    # ── Patient Health Records ────────────────────────────────────────────────
    print("\n" + "█" * 60)
    print("  DATASET 2 – Patient Health Records (GX)")
    print("█" * 60)

    gxv2 = GXDataValidator(suite_name="health_suite")
    # TODO: add calls similar to above for health dataset


if __name__ == "__main__":

    orders = pd.read_csv(
        '../datasets/ecommerce_orders.csv', parse_dates=['order_timestamp']
    )
    health = pd.read_csv(
        '../datasets/patient_health_records.csv', parse_dates=['admission_date']
    )

    # ── E-Commerce Orders ─────────────────────────────────────────────────────
    validate_ecommerce_dataset()

    # ── Patient Health Records ────────────────────────────────────────────────
    validate_patients_dataset()
