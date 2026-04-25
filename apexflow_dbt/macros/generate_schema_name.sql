{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# This ensures 'gold' becomes 'apexflow_gold' and 'silver' becomes 'apexflow_silver' #}
        apexflow_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}