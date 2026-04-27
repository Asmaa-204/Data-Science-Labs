"""
Data Validation - Version 2: Great Expectations v1.x (Modern API)
==================================================================
Applied Data Science - Cairo University

Works with: great-expectations >= 1.0.0
Install:    pip install great-expectations pandas

The GX 1.x API changed significantly from older versions
"""

import pandas as pd
import numpy as np
import great_expectations as gx


def run_validation(df: pd.DataFrame):
    """
    Run data validation using Great Expectations v1.x API.
    """

    # Step 1: Create an in-memory GX context (no files written to disk)
    context = gx.get_context(mode="ephemeral")

    # Step 2: Connect GX to your pandas DataFrame
    data_source = context.data_sources.add_pandas(name="my_pandas_source")
    data_asset = data_source.add_dataframe_asset(name="users_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe("my_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Step 3: Create an Expectation Suite (a named collection of rules)
    suite = context.suites.add(gx.ExpectationSuite(name="users_validation_suite"))

    # ── DIMENSION 1: ACCURACY (business rules) ───────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="age", min_value=0, max_value=120
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(column="email", regex=r".+@.+")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="user_id", min_value=1)
    )

    # ── DIMENSION 2: COMPLETENESS ─────────────────────────────────────────
    for col in ["user_id", "email", "name"]:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

    # ── DIMENSION 3: UNIQUENESS ───────────────────────────────────────────
    for col in ["user_id", "email"]:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column=col))

    # ── DIMENSION 4: CONSISTENCY (schema) ────────────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["user_id", "name", "email", "age", "signup_date", "country"]
        )
    )

    # ── DIMENSION 5: CATEGORICAL (allowed values) ─────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="country", value_set=["USA", "UK", "Canada", "Australia", "Germany"]
        )
    )

    # ── DIMENSION 6: DISTRIBUTION ─────────────────────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectColumnMeanToBeBetween(
            column="age", min_value=30, max_value=60
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=100, max_value=100000)
    )

    # Step 4: Create a Validation Definition (links batch to suite)
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="users_validation", data=batch_def, suite=suite)
    )

    # Step 5: Run validation
    results = validation_def.run(batch_parameters={"dataframe": df})

    # Step 6: Print readable report
    _print_report(results)

    context.build_data_docs()
    context.open_data_docs()

    return results


def _print_report(results):
    """Print a clean summary of GX v1.x validation results."""

    # GX 1.x stores results differently from 0.x
    result_dict = results.describe()
    success = results.success

    print("=" * 58)
    print("    DATA VALIDATION REPORT  (Great Expectations v1.x)")
    print("=" * 58)
    print(f"  Overall Result : {'PASSED' if success else 'FAILED'}")
    print("=" * 58)

    # Iterate through each expectation result
    for exp_result in results.results:
        exp_type = exp_result.expectation_config.type
        col = exp_result.expectation_config.kwargs.get("column", "table-level")
        passed = exp_result.success
        status = "PASS" if passed else "FAIL"

        print(f"\n[{status}] {exp_type}")
        print(f"   Column : {col}")

        if not passed and exp_result.result:
            r = exp_result.result
            if r.get("unexpected_count"):
                print(f"   Issues : {r['unexpected_count']} unexpected values")
            if r.get("partial_unexpected_list"):
                print(f"   Sample : {r['partial_unexpected_list'][:3]}")

    print("\n" + "=" * 58)


# ─────────────────────────────────────────────
# Load DATA
# ─────────────────────────────────────────────

df = pd.read_csv('users.csv', parse_dates=['signup_date'])

# ─────────────────────────────────────────────
# RUN VALIDATION
# ─────────────────────────────────────────────
run_validation(df)
