{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {%- if 'staging' in node.path -%}
            staging
        {%- elif 'intermediate' in node.path -%}
            intermediate
        {%- elif 'mart' in node.path -%}
            mart
        {%- else -%}
            {{ target.schema }}
        {%- endif -%}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}