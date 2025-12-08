### Bugs del entorno  y el código entregado
- En mac hay que setear OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES como variable porque airflow lo usa los subprocesos que no son "fork safe",. 
Al usar subprocesos para llamar a dbt se importan librerias nativas de duckdb, dbt compiladas con Objetive-C que no son fork-safe. 
- El código parece ser para una version vieja de airflow que no soporta las sugerencias que se hicieron para pasar los argumentos, por lo tanto se modifica todo el código.
- En mac para correr el dag hubo que modificar /etc/host agregando la entrada 127.0.0.1   joses-macbook-pro.local
- ds_nodash ya no está disponible en el contexto de Jinja por defecto
Se recomienda usar logical_date que es un objeto datetime que representa la fecha lógica de ejecución
Usamos .strftime('%Y%m%d') para obtener el formato sin guiones (ej: 20251201)
-  No se pudieron usar las recomendaciones porque no son compatibles con las versiones de Airflow usadas en este entorno de evaluación



### Mejoras
- Usar postgres u otra base de datos mas robusta
