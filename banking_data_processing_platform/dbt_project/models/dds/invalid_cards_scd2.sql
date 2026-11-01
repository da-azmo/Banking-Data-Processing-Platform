{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , status
        , account_id
        , "number"
        , term
        , created_at
        , updated_at
        , dwh_loaded_at
    from {{ ref("cards") }} cards
    where not exists (
        select 1
        from {{ ref('bank_accounts_scd2') }} bank_accounts_scd2
        where bank_accounts_scd2.account_bk = cards.account_id and bank_accounts_scd2.is_current
    )

    {% if is_incremental() %}
    and dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
)

select 
    id
    , status
    , account_id
    , "number"
    , term
    , created_at
    , updated_at
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , status
    , account_id
    , "number"
    , term
    , created_at
    , updated_at
    , dwh_loaded_at
from {{ ref('invalid_cards') }}

{% if is_incremental() %}
where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
{% endif %}