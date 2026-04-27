# Import great_expectations and request a Data Context.
import great_expectations as gx

import pandas as pd

# Download and read the sample data into a Pandas DataFrame.
df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

# Optional. Request a File Data Context from a specific folder.
context = gx.get_context(mode="file", project_root_dir="./gx_project")

# Optional. Review the configuration of the returned File Data Context.
print(context)

# PandasDatasource
data_source = context.data_sources.add_pandas("pandas")
# DataFrameAsset
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
# BatchDefinition
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
# Batch
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# ExpectColumnValuesToBeBetween
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6, severity="warning"
)

# ExpectationValidationResult
validation_result = batch.validate(expectation)
# print(validation_result)


# Create an Expectation Suite.
# ExpectationSuite
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="expectations")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6, severity="warning"
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="fare_amount", min_value=0, severity="critical"
    )
)

# Create an Validation Definition.
# ValidationDefinition
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="validation definition",
        data=batch_definition,
        suite=suite,
    )
)

results = validation_definition.run(batch_parameters={"dataframe": df})
print(results)

context.build_data_docs()
context.open_data_docs()
