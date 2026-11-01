{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='client_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as client_bk
        , first_name
        , last_name
        , middle_name
        , birth_date
        , is_deleted
        , created_at
        , updated_at
        , dwh_loaded_at
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(concat_ws('|',
            coalesce(id::text, ''),
            coalesce(first_name::text, ''),
            coalesce(last_name::text, ''),
            coalesce(middle_name::text, ''),
            coalesce(birth_date::text, ''),
            coalesce(is_deleted::text, ''))) as hashdiff
    from {{ ref("clients") }}

    {% if is_incremental() %}
    where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
),
insert_data as (
    select
        nextval('dds.seq_client_sk')::bigint as client_sk
        , ods_data.client_bk
        , ods_data.first_name
        , ods_data.last_name
        , ods_data.middle_name
        , ods_data.birth_date
        , ods_data.is_deleted
        , ods_data.created_at
        , ods_data.updated_at
        , ods_data.dwh_loaded_at
        , ods_data.valid_from
        , ods_data.valid_to
        , ods_data.is_current
        , ods_data.hashdiff
    from ods_data

    {% if is_incremental() %}
    where not exists
    (
        select 1
        from {{ this }} this
        where ods_data.client_bk = this.client_bk and ods_data.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.client_sk
        , this.client_bk
        , this.first_name
        , this.last_name
        , this.middle_name
        , this.birth_date
        , this.is_deleted
        , this.created_at
        , this.updated_at
        , this.dwh_loaded_at
        , this.valid_from
        , {{ current_timestamp() }} as valid_to
        , false as is_current
        , this.hashdiff
    from ods_data
    join {{ this }} this on ods_data.client_bk = this.client_bk and ods_data.hashdiff != this.hashdiff and this.is_current
)
{% endif %}
select 
    client_sk
    , client_bk
    , first_name
    , last_name
    , middle_name
    , birth_date
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
    client_sk
    , client_bk
    , first_name
    , last_name
    , middle_name
    , birth_date
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