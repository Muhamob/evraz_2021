import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from evraz.settings import Connection


class DBFeatureExtractor(TransformerMixin, BaseEstimator):
    query_template = """
    with chronom_features as (
    select target."NPLV",
           sum(coalesce(chronom."O2", 0))                                   "sum_O2",
           coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Нагрев лома'), 0)                  as lom_nagrev_total_minutes,
           -- наличие торкретирования
           count(*) filter (where chronom."NOP" = 'Полусухое торкрет.')  as torcr_count,
           -- общее время слива шлака
           coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter ( where chronom."NOP" = 'Слив шлака'), 0)                  as sliv_shlaka_total_sec,
           -- проводилось ли наведение гарнисажа
           count(*) filter (where chronom."NOP" = 'Наведение гарнисажа') as garnisazh_cnt,
           -- общее время обрыва горловины
           coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Обрыв горловины'), 0)              as obr_gorl_total_sec,
           -- общее время Отсутствие O2
           coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Отсутствие O2'), 0)               as ots_02_total_sec
    from {target} target
             left join chronom_{mode} chronom using ("NPLV")
    group by target."NPLV"
),
     static_features as (
         select *
         from {target} target
                  left join plavki_{mode} plavki using ("NPLV")
                  left join chugun_{mode} chugun using ("NPLV")
), sip_features as (
    select "NPLV",
           -- кол-во, сумма, среднее, отклонение, скошенность засыпанного Уголь ТО
           count("VSSYP") filter (where "NMSYP" = 'Уголь ТО') ugol_cnt,
           coalesce(sum("VSSYP") filter (where "NMSYP" = 'Уголь ТО'), 0) ugol_sum,
           coalesce(avg("VSSYP") filter (where "NMSYP" = 'Уголь ТО'), 0) ugol_avg,
           coalesce(stddev("VSSYP") filter (where "NMSYP" = 'Уголь ТО'), 0) ugol_std,
           -- кол-во, сумма, среднее, отклонение, скошенность засыпанного ФЛЮМАГ
           count("VSSYP") filter (where "NMSYP" = 'ФЛЮМАГ') flumag_cnt,
           coalesce(sum("VSSYP") filter (where "NMSYP" = 'ФЛЮМАГ'), 0) flumag_sum,
           coalesce(avg("VSSYP") filter (where "NMSYP" = 'ФЛЮМАГ'), 0) flumag_avg,
           coalesce(stddev("VSSYP") filter (where "NMSYP" = 'ФЛЮМАГ'), 0) flumag_std,
           -- кол-во, сумма, среднее, отклонение, скошенность засыпанного изв_ЦОИ
           count("VSSYP") filter (where "NMSYP" = 'изв_ЦОИ') uzvcoi_cnt,
           coalesce(sum("VSSYP") filter (where "NMSYP" = 'изв_ЦОИ'), 0) uzvcoi_sum,
           coalesce(avg("VSSYP") filter (where "NMSYP" = 'изв_ЦОИ'), 0) uzvcoi_avg,
           coalesce(stddev("VSSYP") filter (where "NMSYP" = 'изв_ЦОИ'), 0) uzvcoi_std,
           -- кол-во, сумма, среднее, отклонение, скошенность засыпанного Флюс ФОМИ
           count("VSSYP") filter (where "NMSYP" = 'Флюс ФОМИ') flusfomi_cnt,
           coalesce(sum("VSSYP") filter (where "NMSYP" = 'Флюс ФОМИ'), 0) flusfomi_sum,
           coalesce(avg("VSSYP") filter (where "NMSYP" = 'Флюс ФОМИ'), 0) flusfomi_avg,
           coalesce(stddev("VSSYP") filter (where "NMSYP" = 'Флюс ФОМИ'), 0) flusfomi_std
    from sip_{mode} sip
    -- from sip_test sip
    group by "NPLV"
)
select * from static_features
left join chronom_features using("NPLV")
left join sip_features using("NPLV")
where static_features."NPLV" != 511135
    """

    def __init__(self, conn: Connection):
        self.conn = conn

        self.id_column = 'NPLV'
        self.target_columns = ['TST', 'C']
        self.time_columns = None
        self.float_columns = None
        self.cat_columns = None
        self.feature_columns = None
        self.int_columns = None

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

        return self.conn.read_query(query)

    def fit(self, X=None, y=None, **kwargs):
        """
        Происходит инференс типов данных
        """

        df = self._get_df("train", cond="limit 500")

        self.time_columns = df.columns[df.dtypes == 'datetime64[ns]'].difference(self.target_columns + [self.id_column]).tolist()
        self.float_columns = df.columns[df.dtypes == 'float64'].difference(self.target_columns + [self.id_column]).tolist()
        self.int_columns = df.columns[df.dtypes == 'int64'].difference(self.target_columns + [self.id_column]).tolist()
        self.cat_columns = df.columns[df.dtypes == 'object'].difference(self.target_columns + [self.id_column]).tolist()

        self.feature_columns = self.float_columns + self.int_columns + self.cat_columns
        print("Feature columns", self.feature_columns)

        return self

    def transform(self, X=None, mode: str = 'train'):
        df = self._get_df(mode).astype({n: pd.CategoricalDtype() for n in self.cat_columns})

        return df if mode == 'test' else df.dropna(subset=self.target_columns)
