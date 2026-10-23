{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
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
    where id is null or 
        status is null or 
        account_id is null or
        number is null or 
        term is null or
        (length(number) != 16)
),
union_data as (
    select
        id
        , status
        , account_id
        , number
        , term
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , status
        , account_id
        , number
        , term
        , created_at
        , updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_cards") }}
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
from union_data