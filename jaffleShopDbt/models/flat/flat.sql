{{ config(
    materialized='incremental',
    unique_key='fact_id'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders')}}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers')}}
),
stores AS (
    SELECT * FROM {{ ref('stg_stores')}}
),
items AS (
    SELECT * FROM {{ ref('stg_items')}}
),
products AS (
    SELECT * FROM {{ ref('stg_products')}}
),
supplies AS (
    SELECT * FROM {{ ref('stg_supplies')}}
),
tweets AS (
    SELECT * FROM {{ ref('stg_tweets')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'orders.order_id',
        'items.item_id',
        'customers.customer_id'
    ]) }}::text as fact_id, -- unique key

    orders.order_id,
    orders.ordered_at,
    orders.subtotal,
    orders.tax_paid,
    orders.order_total,

    customers.customer_id,
    customers.customer_name,

    stores.store_id,
    stores.store_name,
    stores.store_tax_rate,
    stores.opened_at,

    items.item_id,
    items.product_id,

    products.product_name,
    products.type AS product_type,
    products.product_price,
    products.product_description,

    supplies.supply_id,
    supplies.supply_name,
    supplies.supply_cost,
    supplies.perishable,

    tweets.tweet_id,
    tweets.content AS tweet_content,
    tweets.tweeted_at

FROM orders
LEFT JOIN customers ON orders.customer_id = customers.customer_id
LEFT JOIN stores ON orders.store_id = stores.store_id
LEFT JOIN items ON orders.order_id = items.order_id
LEFT JOIN products ON items.product_id = products.product_id
LEFT JOIN supplies ON supplies.product_id = products.product_id
LEFT JOIN tweets ON tweets.customer_id = customers.customer_id

{% if is_incremental() %}
WHERE orders.ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
{% endif %}
limit 1000