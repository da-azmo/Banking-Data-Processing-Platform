{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
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
    from {{ ref("payments") }} payments
    where (
        not exists (
            select 1
            from {{ ref('purchase_categories_scd2') }} purchase_categories_scd2
            where purchase_categories_scd2.category_bk = payments.category_id and purchase_categories_scd2.is_current
        )
        or not exists (
            select 1
            from {{ ref('transactions_scd2') }} transactions_scd2
            where transactions_scd2.transaction_bk = payments.transaction_id
        )
        or not exists (
            select 1
            from {{ ref('card_scd2') }} sender_card_scd2
            where sender_card_scd2.card_bk = payments.card_id_sender and sender_card_scd2.is_current
        )
        or not exists (
            select 1
            from {{ ref('card_scd2') }} recipient_card_scd2
            where recipient_card_scd2.card_bk = payments.card_id_recipient and recipient_card_scd2.is_current
        )
    )

    {% if is_incremental() %}
    and updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
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
from ods_data

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
    , dwh_loaded_at
from {{ ref('invalid_payments') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
    from {{ this }} this
)
{% endif %}