# Ref: https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites?procedure=sample_code
import great_expectations as gx

context = gx.get_context(mode="ephemeral")  # EphemeralDataContext
# Create an in-memory GX context (no files written to disk)

# Use the `pandas_default` Data Source to retrieve a Batch of sample Data from a data file:
file_path = "yellow_tripdata_sample_2019-01.csv"
batch = context.data_sources.pandas_default.read_csv(file_path)

# Create an Expectation Suite
suite_name = "my_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)

# Add the Expectation Suite to the Data Context
suite = context.suites.add(suite)

# Create an Expectation to put into an Expectation Suite
expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")

# Add the previously created Expectation to the Expectation Suite
suite.add_expectation(expectation)

# Add another Expectation to the Expectation Suite.
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
)

print(len(suite.expectations))
print(suite.expectations)
print(suite.expectations[0])
print(suite.expectations[0].expectation_type)
print(
    suite.expectations[0].column
)  # Parameters are stored as attributes of the Expectation object

# Update the configuration of an Expectation, then push the changes to the Expectation Suite
expectation.column = "pickup_location_id"
print(suite.expectations[0].column)
expectation.save()
# save() writes the in-memory state to persistent storage
# — so the change survives beyond the current script run.
# important when mode="file" or mode="cloud", not when mode="ephemeral" like our script

# Retrieve an Expectation Suite from the Data Context
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite2 = context.suites.get(name=existing_suite_name)
print(len(suite2.expectations))
