import numpy as np


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
