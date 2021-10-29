import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator


def metric(y_test, y_pred):
    """
    Business metrics
    """
    delta_c = np.abs(np.array(y_test['C']) - np.array(y_pred['C']))
    hit_rate_c = np.int64(delta_c < 0.02)

    delta_t = np.abs(np.array(y_test['TST']) - np.array(y_pred['TST']))
    hit_rate_t = np.int64(delta_t < 20)

    n_samples = np.size(y_test['C'])

    return np.sum(hit_rate_c + hit_rate_t) / 2 / n_samples


def sklearn_scorer(estimator, X_test: pd.DataFrame, y_test: pd.DataFrame):
    predictions = estimator.predict(X_test)
    return metric(y_test, predictions)
