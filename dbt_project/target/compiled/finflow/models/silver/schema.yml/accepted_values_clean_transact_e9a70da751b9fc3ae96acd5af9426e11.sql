
    
    

with all_values as (

    select
        currency as value_field,
        count(*) as n_records

    from FINFLOW_DB.silver.clean_transactions
    group by currency

)

select *
from all_values
where value_field not in (
    'USD','GBP','EUR','JPY','CHF','AUD','CAD','HKD'
)


