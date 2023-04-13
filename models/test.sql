{{ config(materialized='table') }}

with gold_sales_external as (
    select * from {{ref('efashion_sales_by_state_external_stage') }}
)

select * from gold_sales_external