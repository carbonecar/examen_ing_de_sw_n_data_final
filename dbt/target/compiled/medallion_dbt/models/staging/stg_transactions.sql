


with source as (
    select *
    from read_parquet(
        '/home/nilay/examen_ing_de_sw_n_data_final/data/clean/transactions_20251201_clean.parquet'
    )
)

-- TODO: Completar el modelo para que cree la tabla staging con los tipos adecuados segun el schema.yml.