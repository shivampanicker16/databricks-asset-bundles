-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![test image](files/shivam_panicker/images/DLT_Declarative_SQL.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating pre-aggregated stores

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_payments AS
select 
payment_id,
sum(payment_amount) as total_payments,
payment_date,
payment_type as mode_of_payment
from live.silver_payments
group by payment_id, payment_date, mode_of_payment

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE gold_payments_entities AS
select 
payment_id,
payment_amount,
payment_date,
payment_from,
payment_to,
payment_status,
payment_type as mode_of_payment
from stream(live.silver_payments)

-- COMMAND ----------

CREATE STREAMING LIVE VIEW vw_gold_payment_360 AS
select 
payments.payment_id,
payments.payment_amount,
payments.payment_date,
payments.payment_from,
payments.payment_to,
payments.payment_status,
payments.payment_type as mode_of_payment,
payments.payment_event_id,
payments.payment_event_state,
payments.payment_event_date,
payments.payment_delete_score,
postings.transaction_amount,
postings.transaction_time
from stream(live.silver_payments) payments
left outer join live.silver_postings_events postings
on payments.payment_id = postings.payment_id
union all
select 
payments.payment_id,
payments.payment_amount,
payments.payment_date,
payments.payment_from,
payments.payment_to,
payments.payment_status,
payments.payment_type as mode_of_payment,
payments.payment_event_id,
payments.payment_event_state,
payments.payment_event_date,
payments.payment_delete_score,
postings.transaction_amount,
postings.transaction_time
from stream(live.silver_postings_events) postings
left outer join live.silver_payments payments
on postings.payment_id = payments.payment_id;

CREATE OR REFRESH STREAMING TABLE gold_payment_360
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true"
);

APPLY CHANGES INTO
  live.gold_payment_360 
FROM
  stream(live.vw_gold_payment_360)
KEYS
  (payment_id)
SEQUENCE BY
  payment_date
STORED AS
  SCD TYPE 1;
