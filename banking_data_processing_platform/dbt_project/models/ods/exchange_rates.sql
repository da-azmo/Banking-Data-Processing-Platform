{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
    , on_schema_change='fail') }}

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
    where name is not null and
        rate is not null and
        base is not null
),
staging_data_with_row_number as (
    select
        id
        , name
        , rate
        , base
        , created_at
        , updated_at
        , dwh_loaded_at
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
existed_data as (
    select
        id
        , name
        , rate
        , base
        , created_at
        , updated_at
        , dwh_loaded_at
    from staging_data_with_row_number
    where rn = 1
)

select 
    id
    , name
    , rate
    , base
    , created_at
    , updated_at
    , dwh_loaded_at
from existed_data