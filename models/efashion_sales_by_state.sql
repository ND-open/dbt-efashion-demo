{{ config(materialized='table') }}

with gold_sales as (
   select state, sum(sales_revenue) as sales_revenue from raw_efashion group by state
)

select * from gold_sales