{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override dbt's default schema name generation.
        By default dbt concatenates: <profile_dataset>_<custom_schema>
        which produces e.g. netflix_curated_netflix_marts.

        This macro makes dbt use the custom_schema_name exactly as-is
        when it is provided, ignoring the profile dataset entirely.
        When no custom schema is set, it falls back to the profile dataset.
    #}
    {%- if custom_schema_name is none -%}
        {{ target.dataset }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}