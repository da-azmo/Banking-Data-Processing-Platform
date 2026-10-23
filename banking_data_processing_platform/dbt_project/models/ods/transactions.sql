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
staging_data_without_duplicates as (
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
),
existed_data as (
    select
        staging_data_without_duplicates.id
        , staging_data_without_duplicates.card_id_sender
        , staging_data_without_duplicates.transaction_date
        , staging_data_without_duplicates.amount
        , staging_data_without_duplicates.currency
        , staging_data_without_duplicates.type
        , case when this.id is null then staging_data_without_duplicates.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data_without_duplicates
    left join {{ this }} this on staging_data_without_duplicates.id = this.id
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