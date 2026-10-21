{% macro read_bronze_source(table_name) -%}
{#
    Reads Bronze Iceberg Parquet files.
    Defaults to '../iceberg_warehouse' when running from inside dbt_transforms,
    or can be overridden via LAKEHOUSE_DATA_DIR environment variable.
#}
{%- set base_dir = env_var('LAKEHOUSE_DATA_DIR', '../iceberg_warehouse') -%}
read_parquet('{{ base_dir }}/bronze/{{ table_name }}/data/*/*.parquet')
{%- endmacro %}
