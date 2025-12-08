
with base as (
    select * from {{ ref('stg_transactions') }}
)

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.
-- Creamos la tabal fct_customer_transactions segun la definicion en schema.yml
