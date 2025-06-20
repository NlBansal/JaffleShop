{{ 
    config(
        materialized='incremental',
        unique_key='store_id',
        incremental_strategy='merge',
        schema='staging'
    ) 
}}

with source as (
    select * from {{ source('jaffle_shop', 'raw_stores') }}
), 

cleaned as (
    select 
        id as store_id,
        name as store_name,
        tax_rate as store_tax_rate,
        opened_at,
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
