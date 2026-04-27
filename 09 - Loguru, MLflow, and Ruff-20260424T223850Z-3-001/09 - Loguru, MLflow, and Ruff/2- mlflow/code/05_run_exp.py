import mlflow
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    # Create the experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    # 1- Either without the with statement:

    # mlflow.start_run()
    # ...
    # mlflow.log_param('learning_rate', 0.01)
    # ...
    # mlflow.end_run()

    # 2- Or better, use the with statement
    with mlflow.start_run(run_name='run_exp', experiment_id=experiment_id) as run:

        # Log a parameter
        mlflow.log_param('learning_rate', 0.01)

        # The 'run.info' object
        print('RUN INFO:')
        print(run.info)
        print()
        print(f'run_id: {run.info.run_id}')
        print(f'experiment_id: {run.info.experiment_id}')
        print(f'status: {run.info.status}')
        print(f'start_time: {run.info.start_time}')
        print(f'end_time: {run.info.end_time}')
        print(f'lifecycle_stage: {run.info.lifecycle_stage}')
