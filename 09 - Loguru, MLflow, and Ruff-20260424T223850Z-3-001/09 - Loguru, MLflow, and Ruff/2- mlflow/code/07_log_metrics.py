import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    # Create experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    # Run experiment
    with mlflow.start_run(run_name='log_metrics', experiment_id=experiment_id) as run:

        # Log 1 metric
        mlflow.log_metric('acc', 90.0)

        # Log many metrics: log_metric's' and metrics dict
        metrics = {'precision': 95.0, 'recall': 85.0, 'f1': 89.7}
        mlflow.log_metrics(metrics)
