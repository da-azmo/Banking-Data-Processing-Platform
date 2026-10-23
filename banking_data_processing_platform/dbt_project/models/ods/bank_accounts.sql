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
        , document_id::text
        , is_deleted::boolean
        , created_at::timestamptz
        , updated_at::timestamptz
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "bank_accounts") }}
    where id is not null and
        status is not null and
        document_id is not null
),
existed_data as (
    select
        staging_data.id
        , staging_data.status
        , staging_data.document_id
        , staging_data.is_deleted
        , staging_data.created_at
        , staging_data.updated_at
        , case when this.id is null then staging_data.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data
    left join {{ this }} this on staging_data.id = this.id
)
select 
    id
    , status
    , document_id
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from existed_data