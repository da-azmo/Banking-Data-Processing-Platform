{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , card_id_sender
        , transaction_date
        , amount
        , currency
        , "type"
        , dwh_loaded_at
    from {{ ref("transactions") }} transactions
    where not exists (
        select 1
        from {{ ref('cards_scd2') }} cards_scd2
        where cards_scd2.card_bk = transactions.card_id_sender and cards_scd2.is_current
    )

    {% if is_incremental() %}
    and dwh_loaded_at >= (
        select coalesce(max(this.dwh_loaded_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
)

select 
    id
    , card_id_sender
    , transaction_date
    , amount
    , currency
    , "type"
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , card_id_sender
    , transaction_date
    , amount
    , currency
    , "type"
    , dwh_loaded_at
from {{ ref('invalid_transactions_scd2') }}

{% if is_incremental() %}
where dwh_loaded_at >= (
    select coalesce(max(this.dwh_loaded_at), '1900-01-01 00:00:00'::timestamptz)
    from {{ this }} this
)
{% endif %}