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
existed_data as (
    select
        staging_data.id
        , staging_data.status
        , staging_data.account_id
        , staging_data.number
        , staging_data.term
        , staging_data.created_at
        , staging_data.updated_at
        , case when this.id is null then staging_data.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data
    left join {{ this }} this on staging_data.id = this.id
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