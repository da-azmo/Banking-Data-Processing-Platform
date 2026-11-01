{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
    , on_schema_change='fail') }}

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
    where id is not null and
        first_name is not null and
        last_name is not null and
        birth_date is not null
),
staging_data_with_row_number as (
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
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
existed_data as (
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
    from staging_data_with_row_number
    where rn = 1
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
from existed_data