{% macro generate_surrogate_key(field_list) -%}
{#
    IceFlow Core SHA-256 Surrogate Key Generator
    Generates a deterministic SHA-256 hexadecimal surrogate key from a list of columns.
    Safely handles NULL values by substituting with a reserved sentinel token '_null_'.
#}
sha256(
    concat_ws('||',
        {%- for field in field_list %}
        coalesce(cast({{ field }} as varchar), '_null_')
        {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    )
)
{%- endmacro %}
