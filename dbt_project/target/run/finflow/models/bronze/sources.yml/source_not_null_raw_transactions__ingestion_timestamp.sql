select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select _ingestion_timestamp
from FINFLOW_DB.RAW.transactions
where _ingestion_timestamp is null



      
    ) dbt_internal_test