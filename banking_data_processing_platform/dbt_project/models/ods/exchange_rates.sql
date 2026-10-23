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
existed_data as (
    select
        staging_data.id
        , staging_data.name
        , staging_data.rate
        , staging_data.base
        , staging_data.created_at
        , staging_data.updated_at
        , case when this.id is null then staging_data.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data
    left join {{ this }} this on staging_data.id = this.id
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