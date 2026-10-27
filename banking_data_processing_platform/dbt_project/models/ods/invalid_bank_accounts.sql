{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='append_new_columns') }}

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
    where id is null or
        status is null or
        document_id is null
),
union_data as (
    select
        id
        , status
        , document_id
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , status
        , document_id
        , is_deleted
        , created_at
        , updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_bank_accounts") }}
)
select 
    id
    , status
    , document_id
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from union_data