{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='payment_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as payment_bk
        , category_id as category_bk
        , transaction_id as transaction_bk
        , card_id_sender as card_sender_bk
        , card_id_recipient as card_recipient_bk
        , payment_date
        , status
        , payment_method
        , created_at
        , updated_at
        , dwh_loaded_at
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(concat_ws('|',
            coalesce(id::text, ''),
            coalesce(category_id::text, ''),
            coalesce(transaction_id::text, ''),
            coalesce(card_id_sender::text, ''),
            coalesce(card_id_recipient::text, ''),
            coalesce(payment_date::text, ''),
            coalesce(status::text, ''),
            coalesce(payment_method::text, ''))) as hashdiff
    from {{ ref("payments") }}

    {% if is_incremental() %}
    where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
),
data_with_fk as (
    select
        ods_data.payment_bk
        , purchase_categories_scd2.category_sk as category_sk
        , transactions_scd2.transaction_sk as transaction_sk
        , sender_cards_scd2.card_sk as card_sk_sender
        , recipient_cards_scd2.card_sk as card_sk_recipient
        , ods_data.payment_date
        , ods_data.status
        , ods_data.payment_method
        , ods_data.created_at
        , ods_data.updated_at
        , ods_data.dwh_loaded_at
        , ods_data.valid_from
        , ods_data.valid_to
        , ods_data.is_current
        , ods_data.hashdiff
    from ods_data
    join {{ ref('purchase_categories_scd2') }} purchase_categories_scd2 on
        purchase_categories_scd2.category_bk = ods_data.category_bk and purchase_categories_scd2.is_current
    join {{ ref('transactions_scd2') }} transactions_scd2 on
        transactions_scd2.transaction_bk = ods_data.transaction_bk
    join {{ ref('cards_scd2') }} sender_cards_scd2 on
        sender_cards_scd2.card_bk = ods_data.card_sender_bk and sender_cards_scd2.is_current
    join {{ ref('cards_scd2') }} recipient_cards_scd2 on
        recipient_cards_scd2.card_bk = ods_data.card_recipient_bk and recipient_cards_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_payment_sk')::bigint as payment_sk
        , data_with_fk.payment_bk
        , data_with_fk.category_sk
        , data_with_fk.transaction_sk
        , data_with_fk.card_sk_sender
        , data_with_fk.card_sk_recipient
        , data_with_fk.payment_date
        , data_with_fk.status
        , data_with_fk.payment_method
        , data_with_fk.created_at
        , data_with_fk.updated_at
        , data_with_fk.dwh_loaded_at
        , data_with_fk.valid_from
        , data_with_fk.valid_to
        , data_with_fk.is_current
        , data_with_fk.hashdiff
    from data_with_fk

    {% if is_incremental() %}
    where not exists
    (
        select 1
        from {{ this }} this
        where data_with_fk.payment_bk = this.payment_bk and data_with_fk.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.payment_sk
        , this.payment_bk
        , this.category_sk
        , this.transaction_sk
        , this.card_sk_sender
        , this.card_sk_recipient
        , this.payment_date
        , this.status
        , this.payment_method
        , this.created_at
        , this.updated_at
        , this.dwh_loaded_at
        , this.valid_from
        , {{ current_timestamp() }} as valid_to
        , false as is_current
        , this.hashdiff
    from data_with_fk
    join {{ this }} this on data_with_fk.payment_bk = this.payment_bk and data_with_fk.hashdiff != this.hashdiff and this.is_current
)
{% endif %}
select 
    payment_sk
    , payment_bk
    , category_sk
    , transaction_sk
    , card_sk_sender
    , card_sk_recipient
    , payment_date
    , status
    , payment_method
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from insert_data
{% if is_incremental() %}
union all
select 
    payment_sk
    , payment_bk
    , category_sk
    , transaction_sk
    , card_sk_sender
    , card_sk_recipient
    , payment_date
    , status
    , payment_method
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from update_data
{% endif %}