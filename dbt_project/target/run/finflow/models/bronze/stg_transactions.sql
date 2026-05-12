
  create or replace   view FINFLOW_DB.bronze.stg_transactions
  
   as (
    

with source as (

    select * from FINFLOW_DB.RAW.transactions

),

typed as (

    select
        transaction_id,
        customer_id,
        upper(trim(transaction_type))   as transaction_type,
        amount::decimal(18,4)           as amount,
        upper(trim(currency))           as currency,
        upper(trim(status))             as status,
        upper(trim(counterparty))       as counterparty,
        trim(reference_number)          as reference_number,
        upper(trim(source_system))      as source_system,
        trim(batch_id)                  as batch_id,
        fee_amount::decimal(18,6)       as fee_amount,
        upper(trim(fee_currency))       as fee_currency,

        -- Parsed timestamps
        transaction_date_ts             as transaction_date,
        settlement_date_ts              as settlement_date,
        created_at_ts                   as created_at,

        -- Partition helpers
        date_trunc('day',  transaction_date_ts) as transaction_day,
        date_trunc('month',transaction_date_ts) as transaction_month,

        -- DQ flags from ETL
        dq_null_transaction_id,
        dq_null_customer_id,
        dq_null_amount,
        dq_negative_amount,
        dq_invalid_currency,
        dq_invalid_status,
        dq_has_issues,

        -- Audit columns
        _ingestion_timestamp,
        _pipeline_layer,
        _pipeline_version,
        _run_id

    from source

)

select * from typed
  );

