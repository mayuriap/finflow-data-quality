

with bronze as (

    select * from FINFLOW_DB.bronze.stg_transactions

),

filtered as (

    select * from bronze
    where
        transaction_id is not null
        and customer_id is not null
        and amount is not null
        and amount > 0
        and currency in ('USD','GBP','EUR','JPY','CHF','AUD','CAD','HKD')
        and status in ('COMPLETED','PENDING','FAILED','CANCELLED','PROCESSING')
        and transaction_date is not null

),

deduped as (

    select * from (
        select
            *,
            row_number() over (
                partition by transaction_id
                order by _ingestion_timestamp desc
            ) as _row_num
        from filtered
    )
    where _row_num = 1

),

enriched as (

    select
        transaction_id,
        customer_id,
        transaction_type,
        amount,
        currency,
        status,
        counterparty,
        reference_number,
        source_system,
        batch_id,
        fee_amount,
        fee_currency,

        case currency
            when 'USD' then amount
            when 'GBP' then amount * 1.27
            when 'EUR' then amount * 1.08
            when 'JPY' then amount * 0.0067
            when 'CHF' then amount * 1.11
            when 'AUD' then amount * 0.65
            when 'CAD' then amount * 0.74
            when 'HKD' then amount * 0.13
            else amount
        end as amount_usd,

        coalesce(fee_amount, 0) as fee_amount_clean,
        amount + coalesce(fee_amount, 0) as total_with_fees,

        case when status = 'COMPLETED' then true else false end as is_completed,
        case when status in ('FAILED','CANCELLED') then true else false end as is_failed,

        transaction_date,
        settlement_date,
        created_at,
        transaction_day,
        transaction_month,
        datediff('day', transaction_date, settlement_date) as settlement_days,

        current_timestamp() as _silver_loaded_at,
        _ingestion_timestamp as _bronze_ingested_at

    from deduped

)

select * from enriched