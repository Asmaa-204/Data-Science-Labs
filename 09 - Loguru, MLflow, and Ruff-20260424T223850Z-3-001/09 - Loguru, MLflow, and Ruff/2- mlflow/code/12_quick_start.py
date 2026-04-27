# =============================================================================

#  REF: https://mlflow.org/docs/latest/ml/getting-started/quickstart/

# ====================== Step 1 - Import MLflow ===============================

import mlflow


mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Add this


mlflow.set_experiment("MLflow Quickstart 2")

# ====================== Step 2 - Prepare training data =======================

import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Load the Iris dataset
X, y = datasets.load_iris(return_X_y=True)

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Define the model hyperparameters
params = {
    "solver": "lbfgs",
    "max_iter": 1000,
    "random_state": 8888,
}


# ====================== Step 3 - Train a model with MLflow Autologging =======

# In this step, we train the model on the training data loaded in the previous step,
# and log the model and its metadata to MLflow.
# The easiest way to do this is to using MLflow's Autologging feature.

# Enable autologging for scikit-learn
mlflow.sklearn.autolog()

# Just train the model normally
lr = LogisticRegression(**params)
lr.fit(X_train, y_train)

# ====================== Step 4 - View the Run in the MLflow UI ===============
# > mlflow server --port 5000
# > then go to: http://localhost:5000

# ====================== Step 5 - Log a model and metadata manually ===========

# Start an MLflow run
with mlflow.start_run():
    # Log the hyperparameters
    mlflow.log_params(params)

    # Train the model
    lr = LogisticRegression(**params)
    lr.fit(X_train, y_train)

    # Log the model
    model_info = mlflow.sklearn.log_model(sk_model=lr, name="iris_model")

    # Predict on the test set, compute and log the loss metric
    y_pred = lr.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    mlflow.log_metric("accuracy", accuracy)

    # Optional: Set a tag that we can use to remind ourselves what this run was for
    mlflow.set_tag("Training Info", "Basic LR model for iris data")

# ===================== Step 6 - Load the model back for inference ============

# Load the model back for predictions
loaded_model = mlflow.sklearn.load_model(model_info.model_uri)

predictions = loaded_model.predict(X_test)

iris_feature_names = datasets.load_iris().feature_names

result = pd.DataFrame(X_test, columns=iris_feature_names)
result["actual_class"] = y_test
result["predicted_class"] = predictions

print(result[:4])
