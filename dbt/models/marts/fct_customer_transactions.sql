{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'transaction_date']
    )
}}

with base as (
    select * from {{ ref('stg_transactions') }}
)

-- Agregamos por cliente Y fecha (permite acumular datos día a día)
select
    customer_id,
    transaction_date,
    count(transaction_id) as transaction_count,
    sum(case when status='completed' then amount else 0 end) as total_amount_completed,
    sum(amount) as total_amount_all
from base
group by customer_id, transaction_date