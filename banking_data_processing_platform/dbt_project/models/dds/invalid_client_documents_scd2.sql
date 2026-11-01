{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , client_id
        , status
        , country
        , document_type
        , series
        , "number"
        , issue_date
        , issuer
        , created_at
        , updated_at
        , dwh_loaded_at
    from {{ ref("client_documents") }} client_documents
    where not exists (
        select 1
        from {{ ref('clients_scd2') }} clients_scd2
        where clients_scd2.client_bk = client_documents.client_id and clients_scd2.is_current
    )

    {% if is_incremental() %}
    and dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
)

select 
    id
    , client_id
    , status
    , country
    , document_type
    , series
    , "number"
    , issue_date
    , issuer
    , created_at
    , updated_at
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , client_id
    , status
    , country
    , document_type
    , series
    , "number"
    , issue_date
    , issuer
    , created_at
    , updated_at
    , dwh_loaded_at
from {{ ref('invalid_client_documents') }}

{% if is_incremental() %}
where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
{% endif %}