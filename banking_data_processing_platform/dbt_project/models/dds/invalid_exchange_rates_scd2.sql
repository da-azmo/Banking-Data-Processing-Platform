{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

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
where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
{% endif %}