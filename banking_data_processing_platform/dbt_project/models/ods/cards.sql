{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
    , on_schema_change='fail') }}

with staging_data as (
    select
        id::text
        , status::text
        , account_id::text
        , number::text
        , term::date
        , created_at::timestamptz
        , updated_at::timestamptz
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "cards") }}
    where id is not null and 
        status is not null and 
        account_id is not null and
        number is not null and 
        term is not null and
        (length(number) = 16)
),
staging_data_with_row_number as (
    select
        id
        , status
        , account_id
        , number
        , term
        , created_at
        , updated_at
        , dwh_loaded_at
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
staging_data_without_duplicates as (
    select
        id
        , status
        , account_id
        , number
        , term
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data_with_row_number
    where rn = 1
),
existed_data as (
    select
        staging_data_without_duplicates.id
        , staging_data_without_duplicates.status
        , staging_data_without_duplicates.account_id
        , staging_data_without_duplicates.number
        , staging_data_without_duplicates.term
        , staging_data_without_duplicates.created_at
        , staging_data_without_duplicates.updated_at
        , case when this.id is null then staging_data_without_duplicates.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data_without_duplicates
    left join {{ this }} this on staging_data_without_duplicates.id = this.id
)
select 
    id
    , status
    , account_id
    , number
    , term
    , created_at
    , updated_at
    , dwh_loaded_at
from existed_data