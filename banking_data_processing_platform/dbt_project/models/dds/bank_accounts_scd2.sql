{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='account_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as account_bk
        , status
        , document_id as document_bk
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(concat_ws('|',
            coalesce(id::text, ''),
            coalesce(status::text, ''),
            coalesce(document_id::text, ''),
            coalesce(is_deleted::text, ''))) as hashdiff
    from {{ ref("bank_accounts") }}

    {% if is_incremental() %}
    where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
),
data_with_fk as (
    select
        ods_data.account_bk
        , ods_data.status
        , client_documents_scd2.document_sk as document_sk
        , ods_data.is_deleted
        , ods_data.created_at
        , ods_data.updated_at
        , ods_data.dwh_loaded_at
        , ods_data.valid_from
        , ods_data.valid_to
        , ods_data.is_current
        , ods_data.hashdiff
    from ods_data
    join {{ ref('client_documents_scd2') }} client_documents_scd2 on
        client_documents_scd2.document_bk = ods_data.document_bk and client_documents_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_account_sk')::bigint as account_sk
        , data_with_fk.account_bk
        , data_with_fk.status
        , data_with_fk.document_sk
        , data_with_fk.is_deleted
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
        where data_with_fk.account_bk = this.account_bk and data_with_fk.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.account_sk
        , this.account_bk
        , this.status
        , this.document_sk
        , this.is_deleted
        , this.created_at
        , this.updated_at
        , this.dwh_loaded_at
        , this.valid_from
        , {{ current_timestamp() }} as valid_to
        , false as is_current
        , this.hashdiff
    from data_with_fk
    join {{ this }} this on data_with_fk.account_bk = this.account_bk and data_with_fk.hashdiff != this.hashdiff and this.is_current
)
{% endif %}
select 
    account_sk
    , account_bk
    , status
    , document_sk
    , is_deleted
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
    account_sk
    , account_bk
    , status
    , document_sk
    , is_deleted
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from update_data
{% endif %}