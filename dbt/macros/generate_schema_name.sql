{% macro generate_schema_name(custom_schema_name, node) -%}
    -- Always use the schema name exactly as provided in config/dbt_project.yml
    {{ custom_schema_name | upper }}
{%- endmacro %}
