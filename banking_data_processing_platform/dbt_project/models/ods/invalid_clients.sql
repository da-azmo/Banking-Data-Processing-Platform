{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='ods'
    , on_schema_change='append_new_columns') }}

with staging_data as (
    select
        id::text
        , first_name::text
        , last_name::text
        , middle_name::text
        , birth_date::date
        , is_deleted::boolean
        , created_at::timestamptz
        , updated_at::timestamptz
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "clients") }}
    where id is null or
        first_name is null or
        last_name is null or
        birth_date is null
),
union_data as (
    select
        id
        , first_name
        , last_name
        , middle_name
        , birth_date
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data

    union all

    select
        id
        , first_name
        , last_name
        , middle_name
        , birth_date
        , is_deleted
        , created_at
        , updated_at
        , {{ current_timestamp() }} as dwh_loaded_at
    from {{ source("staging", "invalid_clients") }}
)
select 
    id
    , first_name
    , last_name
    , middle_name
    , birth_date
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from union_data