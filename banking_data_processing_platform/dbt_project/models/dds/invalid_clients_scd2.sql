{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , first_name
        , last_name
        , middle_name
        , birth_date
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
    from {{ ref("clients") }}

    {% if is_incremental() %}
    and updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
)

select 
    id
    , first_name
    , last_name
    , middle_name
    , birth_date
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , first_name
    , last_name
    , middle_name
    , birth_date
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from {{ ref('invalid_clients') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
    from {{ this }} this
)
{% endif %}