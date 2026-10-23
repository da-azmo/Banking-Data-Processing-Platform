{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='fail') }}

with staging_data as (
    select
        id::text
        , category_id::text
        , transaction_id::text
        , card_id_sender::text
        , card_id_recipient::text
        , payment_date::date
        , status::text
        , payment_method::text
        , created_at::timestamptz
        , updated_at::timestamptz
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "payments") }}
    where id is null or
        category_id is null or
        transaction_id is null or
        card_id_sender is null or
        card_id_recipient is null or
        payment_date is null or
        status is null or
        payment_method is null
),
union_data as (
    select
        id
        , category_id
        , transaction_id
        , card_id_sender
        , card_id_recipient
        , payment_date
        , status
        , payment_method
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , category_id
        , transaction_id
        , card_id_sender
        , card_id_recipient
        , payment_date
        , status
        , payment_method
        , created_at
        , updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_payments") }}
)
select 
    id
    , category_id
    , transaction_id
    , card_id_sender
    , card_id_recipient
    , payment_date
    , status
    , payment_method
    , created_at
    , updated_at
    , dwh_loaded_at
from union_data