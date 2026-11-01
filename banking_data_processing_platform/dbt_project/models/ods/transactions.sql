{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
    , on_schema_change='fail') }}

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
    where id is not null and
        card_id_sender is not null and
        amount is not null and
        currency is not null and
        type is not null
),
staging_data_with_row_number as (
    select
        id
        , card_id_sender
        , transaction_date
        , amount
        , currency
        , type
        , dwh_loaded_at
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
existed_data as (
    select
        id
        , card_id_sender
        , transaction_date
        , amount
        , currency
        , type
        , dwh_loaded_at
    from staging_data_with_row_number
    where rn = 1
)

select 
    id
    , card_id_sender
    , transaction_date
    , amount
    , currency
    , type
    , dwh_loaded_at
from existed_data