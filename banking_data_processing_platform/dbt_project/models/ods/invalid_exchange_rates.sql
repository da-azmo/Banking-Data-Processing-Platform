{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='append_new_columns') }}

with staging_data as (
    select
        concat(name, base)::text as id
        , name::text
        , rate::numeric(10, 2)
        , base::text
        , created_at::timestamptz
        , {{ current_timestamp() }} as updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "exchange_rates") }}
    where name is null or
        rate is null or
        base is null
),
union_data as (
    select
        id
        , name
        , rate
        , base
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        concat(name, base)::text as id
        , name
        , rate
        , base
        , created_at
        , {{ current_timestamp() }} as updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_exchange_rates") }}
)
select 
    id
    , name
    , rate
    , base
    , created_at
    , updated_at
    , dwh_loaded_at
from union_data