

with silver as (

    select * from FINFLOW_DB.silver.clean_transactions

),

daily as (

    select
        transaction_day,
        transaction_month,
        currency,
        transaction_type,
        status,
        source_system,

        count(*)                                            as total_transactions,
        count(case when is_completed then 1 end)            as completed_count,
        count(case when is_failed    then 1 end)            as failed_count,

        sum(amount)                                         as total_amount,
        avg(amount)                                         as avg_amount,
        min(amount)                                         as min_amount,
        max(amount)                                         as max_amount,
        sum(amount_usd)                                     as total_amount_usd,
        sum(fee_amount_clean)                               as total_fees,

        count(case when is_completed then 1 end)::float
            / nullif(count(*), 0)                           as success_rate,

        avg(settlement_days)                                as avg_settlement_days,

        current_timestamp()                                 as _gold_loaded_at

    from silver
    group by
        transaction_day,
        transaction_month,
        currency,
        transaction_type,
        status,
        source_system

)

select * from daily