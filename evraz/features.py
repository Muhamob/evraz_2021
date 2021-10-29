import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from evraz.settings import Connection


class DBFeatureExtractor(TransformerMixin, BaseEstimator):
    query_template = """
    select * from {target} target
    left join plavki_{mode} plavki using("NPLV")
    left join chugun_{mode} chugun using("NPLV")
    {cond}
    """

    def __init__(self, conn: Connection):
        self.conn = conn

        self.id_column = 'NPLV'
        self.target_columns = ['TST', 'C']
        self.time_columns = None
        self.float_columns = None
        self.cat_columns = None
        self.feature_columns = None

    def _get_df(self, mode: str, cond: str = "") -> pd.DataFrame:
        if mode == 'train':
            target = 'target_train'
        elif mode == 'test':
            target = 'sample_submission'
            cond += '\norder by "NPLV"'
        else:
            raise TypeError(f"mode must be 'train' or 'test', got {mode}")

        query = self.query_template.format(
            target=target,
            mode=mode,
            cond=cond
        )
        print(query)

        return self.conn.read_query(query)

    def fit(self, X=None, y=None, **kwargs):
        """
        Происходит инференс типов данных
        """

        df = self._get_df("train", cond="limit 500")

        self.time_columns = df.columns[df.dtypes == 'datetime64[ns]'].difference(self.target_columns + [self.id_column]).tolist()
        self.float_columns = df.columns[df.dtypes == 'float64'].difference(self.target_columns + [self.id_column]).tolist()
        self.cat_columns = df.columns[df.dtypes == 'object'].difference(self.target_columns + [self.id_column]).tolist()

        self.feature_columns = self.float_columns + self.cat_columns

        return self

    def transform(self, X=None, mode: str = 'train'):
        df = self._get_df(mode).astype({n: pd.CategoricalDtype() for n in self.cat_columns})

        return df if mode == 'test' else df.dropna(subset=self.target_columns)
