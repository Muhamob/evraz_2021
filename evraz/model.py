from typing import Tuple, Optional, Sequence

from sklearn.base import BaseEstimator, RegressorMixin
from catboost import CatBoostRegressor
import pandas as pd

from evraz.metrics import metric


base_parameters = {
    'random_state': 42,
    'eval_metric': 'MAE',
    'early_stopping_rounds': 10,
    'iterations': 1000
}


class BaselineModel(RegressorMixin, BaseEstimator):
    """Модель регрессии сразу двух параметров

    Простейшая модель, которая использует CatBoost регрессора для каждой из двух переменной
    """
    def __init__(self, model_params: dict):
        self.model_params = model_params
        self.model_params.update(base_parameters)

        self.model_t = CatBoostRegressor(**self.model_params)
        self.model_c = CatBoostRegressor(**self.model_params)

    def fit(self,
            X: pd.DataFrame,
            y: pd.DataFrame,
            eval_set: Optional[Sequence[Tuple[pd.DataFrame, pd.DataFrame]]] = None,
            **kwargs):

        self.model_t.fit(X, y['TST'],
                         eval_set=[(features, target['TST']) for features, target in eval_set],
                         **kwargs)
        self.model_c.fit(X, y['C'],
                         eval_set=[(features, target['C']) for features, target in eval_set],
                         **kwargs)
        return self

    def predict(self,
                X: pd.DataFrame):
        t_predictions = self.model_t.predict(X)
        c_predictions = self.model_c.predict(X)
        return pd.DataFrame.from_dict({
            'TST': t_predictions,
            'C': c_predictions
        })

    def eval_metric(self, X, y):
        y_pred = self.predict(X)
        return {
            'business': metric(y, y_pred)
        }
