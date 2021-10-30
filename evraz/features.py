import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from evraz.settings import Connection


class DBFeatureExtractor(TransformerMixin, BaseEstimator):
    query_template = ""

    def __init__(self, conn: Connection):
        self.conn = conn

        self.id_column = 'NPLV'
        self.target_columns = ['TST', 'C']
        self.time_columns = None
        self.float_columns = None
        self.cat_columns = None
        self.feature_columns = None
        self.int_columns = None

    def get_df(self, mode: str, cond: str = "") -> pd.DataFrame:
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

        df = self.get_df("train", cond="limit 500")

        self.time_columns = df.columns[df.dtypes == 'datetime64[ns]'].difference(
            self.target_columns + [self.id_column, 'tgt_NPLV']).tolist()
        self.float_columns = df.columns[df.dtypes == 'float64'].difference(
            self.target_columns + [self.id_column, 'tgt_NPLV']).tolist()
        self.int_columns = df.columns[df.dtypes == 'int64'].difference(self.target_columns + [self.id_column, 'tgt_NPLV']).tolist()
        self.cat_columns = df.columns[df.dtypes == 'object'].difference(self.target_columns + [self.id_column, 'tgt_NPLV']).tolist()

        self.feature_columns = self.float_columns + self.int_columns + self.cat_columns
        print("Feature columns", self.feature_columns)

        return self

    def transform(self, X=None, mode: str = 'train'):
        df = self.get_df(mode).astype({n: pd.CategoricalDtype() for n in self.cat_columns})
        return df
        # return df if mode == 'test' else df.dropna(subset=self.target_columns)


class AllFeaturesExtractor(DBFeatureExtractor):
    query_template = """
    select *
    from {target}
    where "NPLV" != 511135
    order by "NPLV"
    """

    def __init__(self, conn):
        super().__init__(conn)

        self.feature_extractors = dict(
            static_fe=StaticFeatures(conn),
            chronom_fe=ChronomRawFeatures(conn),
            sip_fe=SipFeatures(conn),
            # gas_fe=GasRawFeatures(conn),
            gas_proc_fe=GasProcessFeatures(conn)
        )

    def fit(self, X=None, y=None, **kwargs):
        self.feature_columns = []

        for name, extractor in self.feature_extractors.items():
            print(f"Fit {name} feature extractor")
            extractor.fit()
            self.feature_columns.extend(extractor.feature_columns)
        return self

    def transform(self, X=None, mode: str = 'train'):
        target = self.get_df(mode)
        if mode == "train":
            target = target.dropna(subset=["TST", "C"])

        for name, extractor in self.feature_extractors.items():
            print(f"Extracting features from {name} extractor")
            features = extractor.transform(mode=mode)
            print(features)
            target = pd.merge(target, features, how='left', on='NPLV')

        return target


class StaticFeatures(DBFeatureExtractor):
    query_template = """
    select target."NPLV" "tgt_NPLV",
           plavki.*,
           chugun.*
    from {target} target
          left join plavki_{mode} plavki using ("NPLV")
          left join chugun_{mode} chugun using ("NPLV")
    {cond}
    """

    def transform(self, X=None, mode: str = 'train'):
        return super().transform(X, mode).drop(columns=["NPLV"]).rename(columns={'tgt_NPLV': 'NPLV'})


class ChronomRawFeatures(DBFeatureExtractor):
    query_template = """
    select target."NPLV",
       --  количество O2 использованного в процессе
       sum(coalesce(chronom."O2", 0)) filter (where chronom."NOP" != 'Додувка на C') "sum_O2",
       -- общее время нагрева лома
       coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" = 'Нагрев лома'), 0)          as         lom_nagrev_total_minutes,
       -- наличие торкретирования
       count(*) filter (where chronom."NOP" = 'Полусухое торкрет.')       as         torcr_count,
       -- общее время слива шлака
       coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
                filter ( where chronom."NOP" = 'Слив шлака'), 0)          as         sliv_shlaka_total_sec,
       -- проводилось ли наведение гарнисажа
       count(*) filter (where chronom."NOP" = 'Наведение гарнисажа')      as         garnisazh_cnt,
       -- общее время обрыва горловины
       coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" = 'Обрыв горловины'), 0)      as         obr_gorl_total_sec,
       -- общее время Отсутствие O2
       coalesce(sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" = 'Отсутствие O2'), 0)        as         ots_02_total_sec,
       -- общее время отсутствия чугуна
       coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" = 'Отсутствие чугуна'), 0)    as         ots_chugun_total_sec,
       -- время простоя из-за неисправностей
       coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" like 'Неиспр.%'), 0)          as         neispr_total_min,
       -- время ожидания чего-то (Ожидание)
       coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" like 'Ожидание%'), 0)         as         ozhidanie_total_min,
       -- время замера положения фурм (Замер положения фурм)
       coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
                filter (where chronom."NOP" = 'Замер положения фурм'), 0) as         zamer_furm_total_min
    from {target} target
             left join chronom_{mode} chronom using ("NPLV")
             left join plavki_{mode} plavki using ("NPLV")
    where chronom."VR_KON" < plavki."plavka_VR_KON"
    group by target."NPLV"
    {cond}
    """


class SipFeatures(DBFeatureExtractor):
    query_template = """
    with grouped_features as (
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
        left join plavki_{mode} plavki using ("NPLV")
        where sip."DAT_OTD" < plavki."plavka_VR_KON"
        group by "NPLV"
    ), numbered as (
    -- проставление порядкого номера процессов внутри одной плавки
        select sip.*,
               row_number() over (partition by "NPLV" order by "DAT_OTD") rank,
               row_number() over (partition by "NPLV" order by "DAT_OTD" desc) rank_inv
        from sip_{mode} sip
        left join plavki_{mode} plavki using ("NPLV")
        where sip."DAT_OTD" < plavki."plavka_VR_KON"
    ), sip_features as (
    -- объединение признаков сыпучих
        select gp.*,
               -- первое название сыпучих
               first_n."NMSYP" first_sip,
               -- последнее название
               last_n."NMSYP" last_sip
        from grouped_features gp
        left join numbered first_n using("NPLV")
        left join numbered last_n using("NPLV")
        where 1=1
        -- первая строка
        and first_n.rank = 1
        -- последняя
        and last_n.rank_inv = 1
    )
    select * from sip_features
    {cond}
    """


class GasRawFeatures(DBFeatureExtractor):
    query_template = """
    select "NPLV",
           -- мин, среднее, макс, откл
           -- объём
           min("V") min_V,
           max("V") max_V,
           avg("V") avg_V,
           stddev("V") srd_V,
           -- температура
           min("T") min_T,
           max("T") max_T,
           avg("T") avg_T,
           stddev("T") srd_T,
           -- AR
           min("AR") min_AR,
           max("AR") max_AR,
           avg("AR") avg_AR,
           stddev("AR") srd_AR,
           -- CO
           min("CO") min_CO,
           max("CO") max_CO,
           avg("CO") avg_CO,
           stddev("CO") srd_CO,
           -- CO2
           min("CO2") min_CO2,
           max("CO2") max_CO2,
           avg("CO2") avg_CO2,
           stddev("CO2") srd_CO2,
           -- H2
           min("H2") min_H2,
           max("H2") max_H2,
           avg("H2") avg_H2,
           stddev("H2") srd_H2,
           -- O2
           min("O2") min_O2,
           max("O2") max_O2,
           avg("O2") avg_O2,
           stddev("O2") srd_O2,
           -- N2
           min("N2") min_N2,
           max("N2") max_N2,
           avg("N2") avg_N2,
           stddev("N2") srd_N2
    from gas_{mode} gas
    left join plavki_{mode} plavki using ("NPLV")
        where gas."Time" < plavki."plavka_VR_KON"
    group by "NPLV"
    {cond}
    """


class GasProcessFeatures(DBFeatureExtractor):
    query_template = """
    select gas."NPLV",
       avg(gas."T") filter ( where chronom."NOP" = 'Продувка' ) avg_t_produvka,
       stddev(gas."T") filter ( where chronom."NOP" = 'Продувка' ) std_t_produvka,
       min(gas."T") filter ( where chronom."NOP" = 'Продувка' ) min_t_produvka,
       percentile_cont(0.1) within group ( order by gas."T" ) filter ( where chronom."NOP" = 'Продувка' ) percentile_10_produvka,
       percentile_cont(0.9) within group ( order by gas."T" ) filter ( where chronom."NOP" = 'Продувка' ) percentile_90_produvka
    -- from gas_train gas
    -- left join chronom_train chronom
    from gas_{mode} gas
    left join chronom_{mode} chronom
    on gas."NPLV" = chronom."NPLV"
    and gas."Time" between chronom."VR_NACH" and chronom."VR_KON"
    left join plavki_{mode} plavki 
    on plavki."NPLV" = gas."NPLV"
        where gas."Time" < plavki."plavka_VR_KON"
    group by gas."NPLV"
    {cond}
    """
