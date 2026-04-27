import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this


if __name__ == '__main__':

    # Get experiement to delete
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

    # Delete by ID
    # mlflow.delete_experiment(experiment_id=experiment.experiment_id)
