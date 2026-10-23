{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='ods'
    , unique_key='id'
    , on_schema_change='fail') }}

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
    where id is not null and
        client_id is not null and
        status is not null and
        country is not null and
        series is not null and
        number is not null and
        issue_date is not null and
        issuer is not null and
        (length(series)=4) and
        (length(number)=6)
),
staging_data_with_row_number as (
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
        , row_number() over (partition by id order by dwh_loaded_at desc) as rn
    from staging_data
),
staging_data_without_duplicates as (
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
    from staging_data_with_row_number
    where rn = 1
),
existed_data as (
    select
        staging_data_without_duplicates.id
        , staging_data_without_duplicates.client_id
        , staging_data_without_duplicates.status
        , staging_data_without_duplicates.country
        , staging_data_without_duplicates.document_type
        , staging_data_without_duplicates.series
        , staging_data_without_duplicates.number
        , staging_data_without_duplicates.issue_date
        , staging_data_without_duplicates.issuer
        , staging_data_without_duplicates.created_at
        , staging_data_without_duplicates.updated_at
        , case when this.id is null then staging_data_without_duplicates.dwh_loaded_at else this.dwh_loaded_at end as dwh_loaded_at
    from staging_data_without_duplicates
    left join {{ this }} this on staging_data_without_duplicates.id = this.id
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
from existed_data