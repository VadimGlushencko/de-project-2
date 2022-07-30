#1. Создадим справочник стоимости доставки в страны shipping_country_rates

DROP TABLE if EXISTS public.shipping_country_rates;

CREATE TABLE public.shipping_country_rates(
shipping_country_id SERIAL NOT NULL,
shipping_country TEXT,
shipping_country_base_rate numeric(14, 3),
PRIMARY KEY (shipping_country_id)
);
INSERT INTO public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select shipping_country, shipping_country_base_rate 
from shipping
group by shipping_country, shipping_country_base_rate;


#2. Создадим справочник тарифов доставки вендора по договору shipping_agreement из данных строки vendor_agreement_description

DROP TABLE if EXISTS public.shipping_agreement;

CREATE TABLE public.shipping_agreement(
agreementid BIGINT NOT NULL,
agreement_number TEXT,
agreement_rate numeric(14,3),
agreement_commission numeric (14,3),
PRIMARY KEY (agreementid)
);

INSERT INTO public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)
select description[1]::bigint, description[2]::text, description[3]::numeric(14,3), description[4]::numeric(14,3)
from (select regexp_split_to_array(vendor_agreement_description , ':+') as description
from shipping) as foo
group by description
order by 1;


#3. Создадим справочник о типах доставки shipping_transfer из строки shipping_transfer_description

DROP TABLE if EXISTS public.shipping_transfer;
CREATE TABLE public.shipping_transfer(
transfer_type_id SERIAL NOT NULL,
transfer_type TEXT,
transfer_model TEXT,
shipping_transfer_rate numeric(14, 3),
PRIMARY KEY (transfer_type_id)
);

INSERT INTO public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select description[1], description[2], shipping_transfer_rate::numeric(14,3)
from (select regexp_split_to_array(shipping_transfer_description , ':+') as description, shipping_transfer_rate
from shipping) as foo
group by 1, 2, 3
order by 1;


#4. Создадим таблицу shipping_info с уникальными доставками shippingid и свяжите её с созданными справочниками shipping_country_rates, shipping_agreement, shipping_transfer и константной информацией о доставке shipping_plan_datetime , payment_amount , vendorid

DROP TABLE IF EXISTS public.shipping_info;

CREATE TABLE public.shipping_info(
shippingid BIGINT,
vendorid BIGINT,
payment_amount NUMERIC(14,3),
shipping_plan_datetime TIMESTAMP,
transfer_type_id BIGINT,
shipping_country_id BIGINT,
agreementid BIGINT,
PRIMARY KEY (shippingid),
FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(shipping_country_id) ON UPDATE CASCADE,
FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid) ON UPDATE CASCADE
);

INSERT INTO shipping_info
SELECT distinct(s.shippingid), s.vendorid, s.payment_amount , s.shipping_plan_datetime , st.transfer_type_id, 
	scr.shipping_country_id, sa.agreementid
FROM shipping s
LEFT JOIN shipping_transfer st ON s.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model
	AND s.shipping_transfer_rate = st.shipping_transfer_rate
LEFT JOIN shipping_country_rates scr ON s.shipping_country = scr.shipping_country
	AND s.shipping_country_base_rate = scr.shipping_country_base_rate
LEFT JOIN shipping_agreement sa ON s.vendor_agreement_description = sa.agreementid || ':' ||  sa.agreement_number || ':' || 
	sa.agreement_rate :: numeric (14,3) || ':' || sa.agreement_commission :: numeric (14,3)
ORDER BY 1;


#5. Создадим таблицу статусов о доставке shipping_status и включите туда информацию из лога shipping (status , state). Добавим туда вычислимую информацию по фактическому времени доставки shipping_start_fact_datetime, shipping_end_fact_datetime . Отразим для каждого уникального shippingid его итоговое состояние доставки

DROP TABLE IF EXISTS public.shipping_status;

CREATE TABLE public.shipping_status(
shippingid BIGINT, 
status TEXT, 
state TEXT,
shipping_start_fact_datetime TIMESTAMP,
shipping_end_fact_datetime TIMESTAMP,
PRIMARY KEY (shippingid)
);

WITH sh AS (
SELECT shippingid, status, state, state_datetime AS max_dt, 
	   ROW_NUMBER() OVER (PARTITION BY shippingid ORDER BY state_datetime desc) rn
FROM public.shipping)
INSERT INTO public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
SELECT sh.shippingid, sh.status, sh.state,
	   shb.state_datetime AS shipping_start_fact_datetime,
	   shr.state_datetime AS shipping_end_fact_datetime
FROM sh
LEFT JOIN public.shipping shr ON shr.shippingid = sh.shippingid AND shr.state = 'recieved'
LEFT JOIN public.shipping shb ON shb.shippingid = sh.shippingid AND shb.state = 'booked'
WHERE sh.rn = 1;


#6. Создадим представление shipping_datamart на основании готовых таблиц для аналитики

CREATE OR REPLACE VIEW public.shipping_datamart AS(
SELECT si.shippingid, si.vendorid, st.transfer_type, 
	DATE_PART('day', age(ss.shipping_end_fact_datetime ,ss.shipping_start_fact_datetime)) AS full_day_at_shipping,
	(CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0 END) AS is_delay,
	(CASE WHEN status = 'finished' THEN 1 ELSE 0 END) AS is_shipping_finish,
	(CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN, 
	EXTRACT(DAY FROM (shipping_end_fact_datetime - shipping_start_fact_datetime)) AS full_day_at_shipping,
	si.payment_amount,
	(payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate)) AS vat,
	payment_amount * sa.agreement_commission AS profit
FROM shipping_info si
LEFT JOIN shipping_transfer st ON si.transfer_type_id = st.transfer_type_id 
LEFT JOIN shipping_status ss ON si.shippingid = ss.shippingid 
LEFT JOIN shipping_country_rates scr ON si.shipping_country_id = scr.shipping_country_id 
LEFT JOIN shipping_agreement sa ON si.agreementid = sa.agreementid 
);

