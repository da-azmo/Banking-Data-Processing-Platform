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
staging_data_without_duplicates as (
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
),
existed_data as (
    select
        staging_data_without_duplicates.id
        , staging_data_without_duplicates.name
        , staging_data_without_duplicates.rate
        , staging_data_without_duplicates.base
        , staging_data_without_duplicates.created_at
        , staging_data_without_duplicates.updated_at
        , case when this.id is null then staging_data_without_duplicates.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data_without_duplicates
    left join {{ this }} this on staging_data_without_duplicates.id = this.id
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