{% test max_value(model, column_name, max_allowed) %}

select *
from {{ model }}
where {{ column_name }} > {{ max_allowed }}

{% endtest %}
