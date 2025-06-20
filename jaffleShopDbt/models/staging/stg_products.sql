{{ 
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='merge',
        schema='staging'
    ) 
}}

with source as (
    select * from {{ source('jaffle_shop', 'raw_products') }}
), 

cleaned as (
    select 
        sku as product_id,
        name as product_name,
        type,
        price as product_price,
        description as product_description,
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
