
{{ 
    config(
        materialized='incremental',
        unique_key='tweet_id',
        incremental_strategy='merge',
        schema='staging'
    ) 
}}

with source as (
    select * from {{ source('jaffle_shop', 'raw_tweets') }}
), 

cleaned as (
    select 
        id as tweet_id,
        content,
        user_id as customer_id,
        tweeted_at,
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
