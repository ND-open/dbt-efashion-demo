{{ config(materialized='table') }}

with gold_sales_external as (
    select 
    t.$4 as state,
    sum(t.$9::numeric(15,2)) as sales_revenue
    from @ext_efashion (file_format => csv) t
    group by state
)

select * from gold_sales_external
