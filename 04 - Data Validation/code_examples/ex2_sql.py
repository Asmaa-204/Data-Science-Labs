# Ref: https://docs.greatexpectations.io/docs/core/introduction/try_gx

import great_expectations as gx

import pandas as pd

# The connection string is used by the Data Source to connect to the cloud Postgres database.
connection_string = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db"

# Create a Data Context.
context = gx.get_context(mode="ephemeral")  # EphemeralDataContext
# Create an in-memory GX context (no files written to disk)

# Connect to data and create a Batch.

# PostgresDatasource **  ==> instead of PandasDatasource
data_source = context.data_sources.add_postgres(
    "postgres db", connection_string=connection_string
)  # needs: pip install sqlalchemy psycopg2

# TableAsset **          ==> instead of DataFrameAsset
data_asset = data_source.add_table_asset(name="taxi data", table_name="nyc_taxi_data")

# BatchDefinition **     ==> _whole_table instead of _whole_dataframe
batch_definition = data_asset.add_batch_definition_whole_table("batch definition")

# Batch
batch = batch_definition.get_batch()

# Batch
print(batch)
# SqlAlchemyBatchData ** ==> instead of PandasBatchData
print(batch.data)
print(batch.data.selectable)

# Create an Expectation.
# ExpectColumnValuesToBeBetween
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6, severity="warning"
)

# Run the following code to validate the sample data against your Expectation and view the results
# ExpectationValidationResult
validation_result = batch.validate(expectation)
print(validation_result)

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

# Create and run a Checkpoint to validate the data based on the supplied Validation Definition.
# Checkpoint
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint", validation_definitions=[validation_definition]
    )
)
# CheckpointResult
checkpoint_result = checkpoint.run()
print(checkpoint_result.describe())
