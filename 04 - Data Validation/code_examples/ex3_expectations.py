# Ref: https://docs.greatexpectations.io/docs/core/define_expectations/test_an_expectation/

import great_expectations as gx

context = gx.get_context(mode="ephemeral")  # EphemeralDataContext
# Create an in-memory GX context (no files written to disk)

# Use the `pandas_default` Data Source to retrieve a Batch of sample Data from a data file:
file_path = "yellow_tripdata_sample_2019-01.csv"
batch = context.data_sources.pandas_default.read_csv(file_path)

# Define the Expectation to test:
expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)

# Test the Expectation:
validation_results = batch.validate(expectation)

# Evaluate the Validation Results:
print(validation_results)

# If needed, adjust the Expectation's preset parameters and test again:
expectation.min_value = 1
expectation.max_value = 3

# Test the modified expectation and review the new Validation Results:
new_validation_results = batch.validate(expectation)
print(new_validation_results)
