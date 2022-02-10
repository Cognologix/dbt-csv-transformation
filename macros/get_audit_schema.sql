{% macro get_audit_schema() %}

   {% set audit_schema = var('DB_TARGET_SCHEMA') + '_' + var('DB_AUDIT_SCHEMA') %}
   {{return(audit_schema) }}

{% endmacro %}