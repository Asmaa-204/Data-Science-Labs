import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this


if __name__ == '__main__':

    # Create the experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)

    # Print info
    print('EXPERIMENT INFO:')
    print(f'Name: {experiment.name}')
    print(f'ID: {experiment.experiment_id}')
    print(f'Artifact Location: {experiment.artifact_location}')
    print(f'Tags: {experiment.tags}')
    print(f'Lifecycle Stage: {experiment.lifecycle_stage}')
    print(f'Creation timestamp: {experiment.creation_time}')
