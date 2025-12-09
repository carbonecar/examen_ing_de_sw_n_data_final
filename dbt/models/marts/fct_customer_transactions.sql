
with base as (
    select * from {{ ref('stg_transactions') }}
),

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.
-- Creamos la tabal fct_customer_transactions segun la definicion en schema.yml

fct_customer_transactions as (
     select
        customer_id,
        count(transaction_id) as transaction_count,
        sum( case when status='completed' then amount else 0 end) as total_amount_completed,
        sum(amount) as total_amount_all
    from stg_transactions
    group by customer_id
)

select * from fct_customer_transactions