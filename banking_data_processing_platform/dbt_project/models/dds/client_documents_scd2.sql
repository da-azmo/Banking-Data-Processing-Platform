{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='document_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as document_bk
        , client_id as client_bk
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
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(coalesce(id::text, '')  || '|' || 
            coalesce(client_id::text, '') || '|' || 
            coalesce(status::text, '') || '|' || 
            coalesce(country::text, '') || '|' || 
            coalesce(document_type::text, '') || '|' || 
            coalesce(series::text, '') || '|' || 
            coalesce(number::text, '') || '|' || 
            coalesce(issue_date::text, '') || '|' || 
            coalesce(issuer::text, '')) as hashdiff
    from {{ ref("client_documents") }}

    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
),
data_with_fk as (
    select
        ods_data.document_bk
        , ods_data.client_sk as client_sk
        , ods_data.status
        , ods_data.country
        , ods_data.document_type
        , ods_data.series
        , ods_data.number
        , ods_data.issue_date
        , ods_data.issuer
        , ods_data.created_at
        , ods_data.updated_at
        , ods_data.dwh_loaded_at
        , ods_data.valid_from
        , ods_data.valid_to
        , ods_data.is_current
        , ods_data.hashdiff
    from ods_data
    join {{ ref('clients_scd2') }} clients_scd2 on
        clients_scd2.client_bk = ods_data.client_bk and clients_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_document_sk')::bigint as document_sk
        , data_with_fk.document_bk
        , data_with_fk.client_sk
        , data_with_fk.status
        , data_with_fk.country
        , data_with_fk.document_type
        , data_with_fk.series
        , data_with_fk.number
        , data_with_fk.issue_date
        , data_with_fk.issuer
        , data_with_fk.created_at
        , data_with_fk.updated_at
        , data_with_fk.dwh_loaded_at
        , data_with_fk.valid_from
        , data_with_fk.valid_to
        , data_with_fk.is_current
        , data_with_fk.hashdiff
    from data_with_fk

    {% if is_incremental() %}
    where not exists
    (
        select 1
        from {{ this }} this
        where data_with_fk.document_bk = this.document_bk and data_with_fk.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.document_sk
        , this.document_bk
        , this.client_sk
        , this.status
        , this.country
        , this.document_type
        , this.series
        , this.number
        , this.issue_date
        , this.issuer
        , this.created_at
        , this.updated_at
        , this.dwh_loaded_at
        , this.valid_from
        , {{ current_timestamp() }} as valid_to
        , false as is_current
        , this.hashdiff
    from data_with_fk
    join {{ this }} this on data_with_fk.client_bk = this.client_bk and data_with_fk.hashdiff != this.hashdiff and this.is_current
)
{% endif %}
select 
    document_sk
    , document_bk
    , client_sk
    , status
    , country
    , document_type
    , series
    , number
    , issue_date
    , issuer
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from insert_data
{% if is_incremental() %}
union all
select 
    document_sk
    , document_bk
    , client_sk
    , status
    , country
    , document_type
    , series
    , number
    , issue_date
    , issuer
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from update_data
{% endif %}