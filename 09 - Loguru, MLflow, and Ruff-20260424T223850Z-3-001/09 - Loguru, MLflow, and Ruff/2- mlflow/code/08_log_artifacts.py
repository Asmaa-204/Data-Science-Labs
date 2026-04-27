import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    # Create experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    # Run experiment
    with mlflow.start_run(run_name='log_artifacts', experiment_id=experiment_id) as run:

        # Log artifact: text files, csv files, etc.

        # Create a dummy file
        with open('dummy_file.txt', 'w') as f:
            f.write('dummy content')

        # Log 1 artifact
        mlflow.log_artifact(local_path='dummy_file.txt', artifact_path='text_files')

        # Log artifact's' from dir
        mlflow.log_artifacts(
            local_dir='./dummy_artifacts', artifact_path='dummy_artifacts'
        )
