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
        , MD5(coalesce(id::text, '') || '|' || 
            coalesce(card_id_sender::text, '') || '|' || 
            coalesce(transaction_date::text, '') || '|' || 
            coalesce(amount::text, '') || '|' || 
            coalesce(type::text, '')) as hashdiff
    from {{ ref("transactions") }}

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} this
        where ods_data.hashdiff = this.hashdiff
    )
    and dwh_loaded_at >= (
        select coalesce(max(this.dwh_loaded_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    {% endif %}
),
data_with_fk as (
    select
        ods_data.transaction_bk
        , cards_scd2.card_sk as card_sk_sender
        , ods_data.transaction_date
        , ods_data.amount
        , ods_data.currency
        , ods_data.type
        , ods_data.dwh_loaded_at
        , ods_data.hashdiff
    from ods_data
    join {{ ref('cards_scd2') }} cards_scd2 on
        cards_scd2.card_bk = ods_data.card_sender_bk and cards_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_trancation_sk')::bigint as trancation_sk
        , data_with_fk.transaction_bk
        , data_with_fk.card_sk_sender
        , data_with_fk.transaction_date
        , data_with_fk.amount
        , data_with_fk.currency
        , data_with_fk.type
        , data_with_fk.dwh_loaded_at
        , data_with_fk.hashdiff
    from data_with_fk
)

select 
    client_sk
    , client_bk
    , first_name
    , last_name
    , middle_name
    , birth_date
    , is_deleted
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from insert_data