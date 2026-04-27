import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    # Create experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    # Run experiment
    with mlflow.start_run(run_name='log_params', experiment_id=experiment_id) as run:

        # Log 1 parameter
        mlflow.log_param('learning_rate', 0.01)

        # Log many parameters: log_param's' and params dict
        params = {
            'epochs': 10,
            'batch_size': 100,
            'loss_function': 'mse',
            'optimizer': 'adam',
        }
        mlflow.log_params(params)
