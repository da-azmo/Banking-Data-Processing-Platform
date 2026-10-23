{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
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
    where id is not null and
        category_id is not null and
        transaction_id is not null and
        card_id_sender is not null and
        card_id_recipient is not null and
        payment_date is not null and
        status is not null and
        payment_method is not null
),
staging_data_with_row_number as (
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
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
staging_data_without_duplicates as (
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
    from staging_data_with_row_number
    where rn = 1
),
existed_data as (
    select
        staging_data_without_duplicates.id
        , staging_data_without_duplicates.category_id
        , staging_data_without_duplicates.transaction_id
        , staging_data_without_duplicates.card_id_sender
        , staging_data_without_duplicates.card_id_recipient
        , staging_data_without_duplicates.payment_date
        , staging_data_without_duplicates.status
        , staging_data_without_duplicates.payment_method
        , staging_data_without_duplicates.created_at
        , staging_data_without_duplicates.updated_at
        , case when this.id is null then staging_data_without_duplicates.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data_without_duplicates
    left join {{ this }} this on staging_data_without_duplicates.id = this.id
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
from existed_data