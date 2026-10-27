{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , "name"
        , rate
        , base
        , created_at
        , updated_at
        , dwh_loaded_at
    from {{ ref("exchange_rates") }}

    {% if is_incremental() %}
    and updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
)

select 
    id
    , "name"
    , rate
    , base
    , created_at
    , updated_at
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , "name"
    , rate
    , base
    , created_at
    , updated_at
    , dwh_loaded_at
from {{ ref('invalid_exchange_rates') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
    from {{ this }} this
)
{% endif %}