# Ref: https://docs.greatexpectations.io/docs/core/introduction/try_gx

import great_expectations as gx

import pandas as pd

# Download and read the sample data into a Pandas DataFrame.
df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

print(df.shape)
print(df.columns)
print(df.index)

# Create a Data Context.
context = gx.get_context(mode="ephemeral")  # EphemeralDataContext
# Create an in-memory GX context (no files written to disk)

# Connect to data and create a Batch.
# A Data Asset is a collection of related records within a Data Source.
# These records may be located within multiple files, but each Data Asset is only capable
# of reading a single specific file format which is determined when it is created.
# However, a Data Source may contain multiple Data Assets covering different file formats and groups of records.

# PandasDatasource
data_source = context.data_sources.add_pandas("pandas")
# DataFrameAsset
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
# BatchDefinition
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
# NOTE: Because dataframes are always provided in their entirety,
# dataframe Batch Definitions always use the add_batch_definition_whole_dataframe() method.
# Batch
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
# In Great Expectations, a batch is a logical unit of data to validate

# To retrieve an existing data_source or data_asset, you can use the following code:
# data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)

# Batch
print(batch)
# PandasBatchData
print(batch.data)
# batch.data.dataframe
print(batch.data.dataframe.head())


# Create an Expectation.
# ExpectColumnValuesToBeBetween
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=5, severity="warning"
)

# Run the following code to validate the sample data against your Expectation and view the results
# ExpectationValidationResult
validation_result = batch.validate(expectation)
print(validation_result)
