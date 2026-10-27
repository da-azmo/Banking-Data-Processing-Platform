{{ config(
    materialized='incremental'
    , incremental_strategy='append'
    , schema='dds'
    , on_schema_change='append_new_columns') }}

with ods_data as (
    select
        id
        , status
        , document_id
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
    from {{ ref("bank_accounts") }} bank_accounts
    where not exists (
        select 1
        from {{ ref('client_documents_scd2') }} client_documents_scd2
        where client_documents_scd2.document_bk = bank_accounts.document_id and client_documents_scd2.is_current
    )

    {% if is_incremental() %}
    and updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
)

select 
    id
    , status
    , document_id
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from ods_data

union all

select 
    id
    , status
    , document_id
    , is_deleted
    , created_at
    , updated_at
    , dwh_loaded_at
from {{ ref('invalid_bank_accounts') }}

{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
    from {{ this }} this
)
{% endif %}