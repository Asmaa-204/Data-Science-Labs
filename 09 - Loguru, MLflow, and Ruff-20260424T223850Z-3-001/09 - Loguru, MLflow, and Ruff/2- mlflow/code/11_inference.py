import mlflow
import pandas as pd

from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

from mlflow.models import infer_signature
from globals import EXPERIMENT_NAME

mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this

if __name__ == '__main__':

    n_samples, n_features = 1000, 10

    # Create classification dataset
    X, y = make_classification(
        n_samples=n_samples,
        n_features=n_features,
        n_informative=5,
        n_redundant=5,
        random_state=42,
    )
    X = pd.DataFrame(X, columns=[f'feature_{i}' for i in range(n_features)])
    y = pd.DataFrame(y, columns=['target'])

    # Take the test set
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Load model
    run_id = 'a74aa6166c014c009699b9c81c05918e'
    model_name = 'random_forest_classifier'
    model_uri = f'runs:/{run_id}/{model_name}'
    rfc = mlflow.sklearn.load_model(model_uri=model_uri)

    # Predict
    y_pred = rfc.predict(X_test)
    y_pred = pd.DataFrame(y_pred, columns=['prediction'])

    print(y_pred.head())
