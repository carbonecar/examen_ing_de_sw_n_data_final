{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

with source as (
    select *
    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
)

-- TODO: Completar el modelo para que cree la tabla staging con los tipos adecuados segun el schema.yml.
-- Creamos la tabla staging stg_transactions segun la definicion en schema.yml
select
    transaction_id,
    customer_id,
    amount,
    status,
    transaction_ts,
    transaction_date,
from source
