{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , unique_key='transaction_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as transaction_bk
        , card_id_sender as card_sender_bk
        , transaction_date
        , amount
        , currency
        , type
        , dwh_loaded_at
        , MD5(concat_ws('|',
            coalesce(id::text, ''),
            coalesce(card_id_sender::text, ''),
            coalesce(transaction_date::text, ''),
            coalesce(amount::text, ''),
            coalesce(type::text, ''))) as hashdiff
    from {{ ref("transactions") }}

    {% if is_incremental() %}
    where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
),
valid_data as (
    select
        ods_data.transaction_bk
        , ods_data.card_sender_bk
        , ods_data.transaction_date
        , ods_data.amount
        , ods_data.currency
        , ods_data.type
        , ods_data.dwh_loaded_at
        , ods_data.hashdiff
    from ods_data
    where not exists (
        select 1
        from {{ this }} this
        where ods_data.hashdiff = this.hashdiff
    )
),
data_with_fk as (
    select
        valid_data.transaction_bk
        , cards_scd2.card_sk as card_sk_sender
        , valid_data.transaction_date
        , valid_data.amount
        , exchange_rates_scd2.rate_sk as rate_sk
        , valid_data.type
        , valid_data.dwh_loaded_at
        , valid_data.hashdiff
    from valid_data
    join {{ ref('cards_scd2') }} cards_scd2 on
        cards_scd2.card_bk = valid_data.card_sender_bk and cards_scd2.is_current
    join {{ ref('exchange_rates_scd2') }} exchange_rates_scd2 on
        exchange_rates_scd2.rate_bk = valid_data.currency and exchange_rates_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_transaction_sk')::bigint as transaction_sk
        , data_with_fk.transaction_bk
        , data_with_fk.card_sk_sender
        , data_with_fk.transaction_date
        , data_with_fk.amount
        , data_with_fk.rate_sk
        , data_with_fk.type
        , data_with_fk.dwh_loaded_at
        , data_with_fk.hashdiff
    from data_with_fk
)

select 
    transaction_sk
    , transaction_bk
    , card_sk_sender
    , transaction_date
    , amount
    , rate_sk
    , "type"
    , dwh_loaded_at
    , hashdiff
from insert_data