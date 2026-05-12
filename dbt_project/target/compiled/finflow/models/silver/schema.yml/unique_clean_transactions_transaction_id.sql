
    
    

select
    transaction_id as unique_field,
    count(*) as n_records

from FINFLOW_DB.silver.clean_transactions
where transaction_id is not null
group by transaction_id
having count(*) > 1


