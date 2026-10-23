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
existed_data as (
    select
        staging_data.id
        , staging_data.first_name
        , staging_data.last_name
        , staging_data.middle_name
        , staging_data.birth_date
        , staging_data.is_deleted
        , staging_data.created_at
        , staging_data.updated_at
        , case when this.id is null then staging_data.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data
    left join {{ this }} this on staging_data.id = this.id
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