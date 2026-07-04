{% macro generate_surrogate_key(field_list) -%}
    -- generate a concatenated string key from a list of columns
    -- using coalesce to ensure nulls don't break the concatenation
    md5(
        {%- for field in field_list -%}
            COALESCE(TRIM(UPPER(CAST({{ field }} AS TEXT))), '__NULL__')
            {%- if not loop.last %} || '-' || {% endif -%}
        {%- endfor -%}
    )
    
{%- endmacro %}