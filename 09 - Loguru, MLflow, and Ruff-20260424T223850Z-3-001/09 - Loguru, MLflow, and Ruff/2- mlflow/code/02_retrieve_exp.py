import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this


if __name__ == '__main__':

    # 1- By ID:
    # experiment = mlflow.get_experiment(EXPERIMENT_ID)

    # 2- By Name:
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

    # Print info
    print('EXPERIMENT INFO:')
    print(f'Name: {experiment.name}')
    print(f'ID: {experiment.experiment_id}')
    print(f'Artifact Location: {experiment.artifact_location}')
    print(f'Tags: {experiment.tags}')
    print(f'Lifecycle Stage: {experiment.lifecycle_stage}')
    print(f'Creation timestamp: {experiment.creation_time}')
