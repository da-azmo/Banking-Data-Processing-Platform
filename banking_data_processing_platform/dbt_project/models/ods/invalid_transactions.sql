{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='append_new_columns') }}

with staging_data as (
    select
        id::text
        , card_id_sender::text
        , transaction_date::timestamptz
        , amount::numeric(20, 2)
        , currency::text
        , type::text
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "transactions") }}
    where id is null or
        card_id_sender is null or
        amount is null or
        currency is null or
        type is null
),
union_data as (
    select
        id
        , card_id_sender
        , transaction_date
        , amount
        , currency
        , type
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , card_id_sender
        , transaction_date
        , amount
        , currency
        , type
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_transactions") }}
)
select 
    id
    , card_id_sender
    , transaction_date
    , amount
    , currency
    , type
    , dwh_loaded_at
from union_data