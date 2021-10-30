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

-- удаление таблиц
DROP TABLE chugun_train;
DROP TABLE plavki_test;
DROP TABLE sip_test;
DROP TABLE gas_test;
DROP TABLE plavki_train;
DROP TABLE sip_train;
DROP TABLE target_train;
DROP TABLE gas_train;
DROP TABLE produv_test;
DROP TABLE chronom_test;
DROP TABLE lom_test;
DROP TABLE produv_train;
DROP TABLE chronom_train;
DROP TABLE chugun_test;
DROP TABLE lom_train;
drop table sample_submission;

-- таблицы созданные для данного соревнования
select CONCAT('DROP TABLE ', TABLE_NAME,';')
FROM INFORMATION_SCHEMA.TABLES
WHERE 1=1
and TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='lake'
and (
	table_name like '%_train'
	or
	table_name like '%_test'
	)

-- схемы таблицы (использовался csvsql из пакета csvkit)
CREATE TABLE chronom_test (
	a DECIMAL NOT NULL,
	"NPLV" DECIMAL NOT NULL,
	"TYPE_OPER" VARCHAR NOT NULL,
	"NOP" VARCHAR NOT NULL,
	"VR_NACH" TIMESTAMP WITHOUT TIME ZONE,
	"VR_KON" TIMESTAMP WITHOUT TIME ZONE,
	"O2" DECIMAL
);

CREATE TABLE chronom_train (
	a DECIMAL NOT NULL,
	"NPLV" DECIMAL NOT NULL,
	"TYPE_OPER" VARCHAR NOT NULL,
	"NOP" VARCHAR NOT NULL,
	"VR_NACH" TIMESTAMP WITHOUT TIME ZONE,
	"VR_KON" TIMESTAMP WITHOUT TIME ZONE,
	"O2" DECIMAL
);

CREATE TABLE chugun_test (
	"NPLV" DECIMAL NOT NULL,
	"VES" DECIMAL NOT NULL,
	"T" DECIMAL NOT NULL,
	"SI" DECIMAL NOT NULL,
	"MN" DECIMAL NOT NULL,
	"S" DECIMAL NOT NULL,
	"P" DECIMAL NOT NULL,
	"CR" DECIMAL NOT NULL,
	"NI" DECIMAL NOT NULL,
	"CU" DECIMAL NOT NULL,
	"V" DECIMAL NOT NULL,
	"TI" DECIMAL NOT NULL,
	"DATA_ZAMERA" TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE chugun_train (
	"NPLV" DECIMAL NOT NULL,
	"VES" DECIMAL NOT NULL,
	"T" DECIMAL NOT NULL,
	"SI" DECIMAL NOT NULL,
	"MN" DECIMAL NOT NULL,
	"S" DECIMAL NOT NULL,
	"P" DECIMAL NOT NULL,
	"CR" DECIMAL NOT NULL,
	"NI" DECIMAL NOT NULL,
	"CU" DECIMAL NOT NULL,
	"V" DECIMAL NOT NULL,
	"TI" DECIMAL NOT NULL,
	"DATA_ZAMERA" TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE gas_test (
	"NPLV" DECIMAL NOT NULL,
	"Time" TIMESTAMP WITHOUT TIME ZONE,
	"V" DECIMAL NOT NULL,
	"T" DECIMAL NOT NULL,
	"O2" DECIMAL NOT NULL,
	"N2" DECIMAL NOT NULL,
	"H2" DECIMAL NOT NULL,
	"CO2" DECIMAL NOT NULL,
	"CO" DECIMAL NOT NULL,
	"AR" DECIMAL NOT NULL,
	"T фурмы 1" DECIMAL NOT NULL,
	"T фурмы 2" DECIMAL NOT NULL,
	"O2_pressure" DECIMAL NOT NULL
);

CREATE TABLE gas_train (
	"NPLV" DECIMAL NOT NULL,
	"Time" TIMESTAMP WITHOUT TIME ZONE,
	"V" DECIMAL NOT NULL,
	"T" DECIMAL NOT NULL,
	"O2" DECIMAL NOT NULL,
	"N2" DECIMAL NOT NULL,
	"H2" DECIMAL NOT NULL,
	"CO2" DECIMAL NOT NULL,
	"CO" DECIMAL NOT NULL,
	"AR" DECIMAL NOT NULL,
	"T фурмы 1" DECIMAL NOT NULL,
	"T фурмы 2" DECIMAL NOT NULL,
	"O2_pressure" DECIMAL NOT NULL
);

CREATE TABLE lom_test (
	"NPLV" DECIMAL NOT NULL,
	"VDL" DECIMAL NOT NULL,
	"NML" VARCHAR NOT NULL,
	"VES" DECIMAL NOT NULL
);

CREATE TABLE lom_train (
	"NPLV" DECIMAL NOT NULL,
	"VDL" DECIMAL NOT NULL,
	"NML" VARCHAR NOT NULL,
	"VES" DECIMAL NOT NULL
);

CREATE TABLE plavki_test (
	"NPLV" DECIMAL NOT NULL,
	"plavka_VR_NACH" TIMESTAMP WITHOUT TIME ZONE,
	"plavka_VR_KON" TIMESTAMP WITHOUT TIME ZONE,
	"plavka_NMZ" VARCHAR NOT NULL,
	"plavka_NAPR_ZAD" VARCHAR NOT NULL,
	"plavka_STFUT" DECIMAL NOT NULL,
	"plavka_TIPE_FUR" VARCHAR NOT NULL,
	"plavka_ST_FURM" DECIMAL NOT NULL,
	"plavka_TIPE_GOL" VARCHAR NOT NULL,
	"plavka_ST_GOL" DECIMAL NOT NULL
);

CREATE TABLE plavki_train (
	"NPLV" DECIMAL NOT NULL,
	"plavka_VR_NACH" TIMESTAMP WITHOUT TIME ZONE,
	"plavka_VR_KON" TIMESTAMP WITHOUT TIME ZONE,
	"plavka_NMZ" VARCHAR NOT NULL,
	"plavka_NAPR_ZAD" VARCHAR NOT NULL,
	"plavka_STFUT" DECIMAL NOT NULL,
	"plavka_TIPE_FUR" VARCHAR NOT NULL,
	"plavka_ST_FURM" DECIMAL NOT NULL,
	"plavka_TIPE_GOL" VARCHAR NOT NULL,
	"plavka_ST_GOL" DECIMAL NOT NULL
);

CREATE TABLE produv_test (
	"NPLV" DECIMAL NOT NULL,
	"SEC" TIMESTAMP WITHOUT TIME ZONE,
	"RAS" DECIMAL NOT NULL,
	"POL" DECIMAL NOT NULL
);

CREATE TABLE produv_train (
	"NPLV" DECIMAL NOT NULL,
	"SEC" TIMESTAMP WITHOUT TIME ZONE,
	"RAS" DECIMAL NOT NULL,
	"POL" DECIMAL NOT NULL
);

CREATE TABLE sample_submission (
	"NPLV" DECIMAL NOT NULL,
	"TST" DECIMAL NOT NULL,
	"C" DECIMAL NOT NULL
);

CREATE TABLE sip_test (
	"NPLV" DECIMAL NOT NULL,
	"VDSYP" DECIMAL NOT NULL,
	"NMSYP" VARCHAR NOT NULL,
	"VSSYP" DECIMAL NOT NULL,
	"DAT_OTD" TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE sip_train (
	"NPLV" DECIMAL NOT NULL,
	"VDSYP" DECIMAL NOT NULL,
	"NMSYP" VARCHAR NOT NULL,
	"VSSYP" DECIMAL NOT NULL,
	"DAT_OTD" TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE target_train (
	"NPLV" DECIMAL NOT NULL,
	"TST" DECIMAL NOT NULL,
	"C" DECIMAL NOT NULL
);
