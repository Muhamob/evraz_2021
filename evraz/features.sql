-- funcs
-- разница во времени в минутах
create or replace function datediff_minutes(end_date timestamp without time zone,
                                            start_date timestamp without time zone) returns integer as
$$
begin
    return (DATE_PART('day', end_date - start_date) * 24 + DATE_PART('hour', end_date - start_date)) * 60
        + DATE_PART('minute', end_date - start_date);
end;
$$ language plpgsql;

-- разница во времени в секундах
create or replace function datediff_seconds(end_date timestamp without time zone,
                                            start_date timestamp without time zone) returns integer as
$$
begin
    return datediff_minutes(end_date, start_date) * 60
    + date_part('second', end_date - start_date);
end;
$$ language plpgsql;


-- базовые признаки
select *
from target_train target
         left join plavki_train plavki using ("NPLV")
         left join chugun_train chugun using ("NPLV");

-- признаки из таблицы chronom
select target."NPLV",
       sum(coalesce(chronom."O2", 0))                  "sum_O2",
       sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
       filter (where chronom."NOP" = 'Нагрев лома') as lom_nagrev_total_minutes,
       -- наличие торкретирования
       count(*) filter (where chronom."NOP" = 'Полусухое торкрет.') as torcr_count,
       -- общее время слива шлака
        sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
        filter ( where chronom."NOP" = 'Слив шлака') as sliv_shlaka_total_sec,
       -- проводилось ли наведение гарнисажа
        count(*) filter (where chronom."NOP" = 'Наведение гарнисажа') as garnisazh_cnt,
        -- общее время обрыва горловины
        sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
        filter (where chronom."NOP" = 'Обрыв горловины') as obr_gorl_total_sec,
       -- общее время Отсутствие O2
        sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
        filter (where chronom."NOP" = 'Отсутствие O2') as ots_02_total_sec
-- from target_train target
--          left join chronom_train chronom using ("NPLV")
from sample_submission target
         left join chronom_test chronom using ("NPLV")
group by target."NPLV"



-- сгруппированные признаки
with chronom_features as (
    select target."NPLV",
           sum(coalesce(chronom."O2", 0))                                   "sum_O2",
           coalesce(sum(datediff_minutes(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Нагрев лома'), 0)                  as lom_nagrev_total_minutes,
           -- наличие торкретирования
           count(*) filter (where chronom."NOP" = 'Полусухое торкрет.')  as torcr_count,
           -- общее время слива шлака
           sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter ( where chronom."NOP" = 'Слив шлака')                  as sliv_shlaka_total_sec,
           -- проводилось ли наведение гарнисажа
           count(*) filter (where chronom."NOP" = 'Наведение гарнисажа') as garnisazh_cnt,
           -- общее время обрыва горловины
           sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Обрыв горловины')              as obr_gorl_total_sec,
           -- общее время Отсутствие O2
           sum(datediff_seconds(chronom."VR_KON", chronom."VR_NACH"))
           filter (where chronom."NOP" = 'Отсутствие O2')                as ots_02_total_sec
    from target_train target
             left join chronom_train chronom using ("NPLV")
    group by target."NPLV"
),
     static_features as (
         select *
         from target_train target
                  left join plavki_train plavki using ("NPLV")
                  left join chugun_train chugun using ("NPLV")
)
select * from static_features
left join chronom_features using("NPLV")