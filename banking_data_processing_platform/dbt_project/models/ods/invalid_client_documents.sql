{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='append_new_columns') }}

with staging_data as (
    select
        id::text
        , client_id::text
        , status::text
        , country::text
        , document_type::text
        , series::text
        , number::text
        , issue_date::date
        , issuer::text
        , created_at::timestamptz
        , updated_at::timestamptz
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "client_documents") }}
    where id is null or
        client_id is null or
        status is null or
        country is null or
        series is null or
        number is null or
        issue_date is null or
        issuer is null or
        (length(series)!=4) or
        (length(number)!=6)
),
union_data as (
    select
        id
        , client_id
        , status
        , country
        , document_type
        , series
        , number
        , issue_date
        , issuer
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , client_id
        , status
        , country
        , document_type
        , series
        , number
        , issue_date
        , issuer
        , created_at
        , updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_client_documents") }}
)
select 
    id
    , client_id
    , status
    , country
    , document_type
    , series
    , number
    , issue_date
    , issuer
    , created_at
    , updated_at
    , dwh_loaded_at
from union_data