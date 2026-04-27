import mlflow
import matplotlib.pyplot as plt

from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

from sklearn.metrics import PrecisionRecallDisplay
from sklearn.metrics import RocCurveDisplay

from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    # Create experiment if it doesn't exist, then sets it as active
    experiment = mlflow.set_experiment(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    # Run experiment
    with mlflow.start_run(run_name='log_images', experiment_id=experiment_id) as run:

        # Create classification dataset
        X, y = make_classification(
            n_samples=1000,
            n_features=10,
            n_informative=5,
            n_redundant=5,
            random_state=42,
        )
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Train classifier and predict test
        rfc = RandomForestClassifier(n_estimators=100, random_state=42)
        rfc.fit(X_train, y_train)
        y_pred = rfc.predict(X_test)

        # Create figure
        fig_pr = plt.figure()
        pr_display = PrecisionRecallDisplay.from_predictions(
            y_test, y_pred, ax=plt.gca()
        )
        plt.title('Precision-Recall Curve')
        plt.legend()

        # Log figure
        mlflow.log_figure(fig_pr, 'metrics/my_precision_recall_curve.png')
