-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![test image](files/shivam_panicker/images/DLT_Data_Quality.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Standardizing the payments dataset

-- COMMAND ----------

CREATE STREAMING LIVE VIEW vw_silver_payments 
AS
select 
  int(replace(payments.ExternalReferenceID, "PAY_", "")) as payment_id,
  payments.PaymentAmount as payment_amount,
  cast(payments.PaymentDate as TIMESTAMP) as payment_date,
  payments.PaymentStatus as payment_status,
  payments.PaymentFrom as payment_from,
  payments.PaymentTo as payment_to,
  payments.PaymentType as payment_type,
  events.paymentEventID as payment_event_id,
  events.EventState as payment_event_state,
  cast(events.EventDate as TIMESTAMP) as payment_event_date,
  events._DeleteScore as payment_delete_score
from stream(live.bronze_payments) payments -- Add comments here
left outer join live.bronze_payments_events events
on payments.ExternalReferenceID = events.ExternalReferenceID
union all
select 
  int(replace(payments.ExternalReferenceID, "PAY_", "")) as payment_id,
  payments.PaymentAmount as payment_amount,
  cast(payments.PaymentDate as TIMESTAMP) as payment_date,
  payments.PaymentStatus as payment_status,
  payments.PaymentFrom as payment_from,
  payments.PaymentTo as payment_to,
  payments.PaymentType as payment_type,
  events.paymentEventID as payment_event_id,
  events.EventState as payment_event_state,
  cast(events.EventDate as TIMESTAMP) as payment_event_date,
  events._DeleteScore as payment_delete_score
from stream(live.bronze_payments_events) events -- Add comments here
left outer join live.bronze_payments payments 
on events.ExternalReferenceID = payments.ExternalReferenceID;

CREATE OR REFRESH STREAMING TABLE silver_payments
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true"
);

APPLY CHANGES INTO
  live.silver_payments 
FROM
  stream(live.vw_silver_payments)
KEYS
  (payment_id)
SEQUENCE BY
  payment_date
STORED AS
  SCD TYPE 2;



-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_postings_events
AS
select 
  int(replace(PostingID, "TRX_", "")) as posting_id,
  int(replace(ExternalReferenceID, "PAY_", "")) as payment_id,
  cast(TransactionAmount as DOUBLE) as transaction_amount,
  cast(TransactionDate as TIMESTAMP) as transaction_time
from stream(live.bronze_postings_events)

-- COMMAND ----------


