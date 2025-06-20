{{ 
    config(
        materialized='incremental',
        unique_key='supply_id',
        incremental_strategy='merge',
        schema='staging'
    ) 
}}

with source as (
    select * from {{ source('jaffle_shop', 'raw_supplies') }}
), 

cleaned as (
    select 
        id as supply_id,
        sku as product_id,
        cost as supply_cost,
        name as supply_name,
        perishable,
        _airbyte_extracted_at as extraction_datetime
    from source
), 

filtered as (
    select *
    from cleaned
    {% if is_incremental() %}
        where extraction_datetime > (
            select coalesce(max(extraction_datetime), '1900-01-01')
            from {{ this }}
        )
    {% endif %}
)

select * from filtered  
