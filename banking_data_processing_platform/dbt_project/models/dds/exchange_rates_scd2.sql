{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='rate_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as rate_bk
        , rate
        , base
        , created_at
        , updated_at
        , dwh_loaded_at
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(concat_ws('|',
            coalesce(id::text, ''),
            coalesce(rate::text, ''),
            coalesce(base::text, ''))) as hashdiff
    from {{ ref("exchange_rates") }}

    {% if is_incremental() %}
    where dwh_loaded_at >= (
        select coalesce(date_trunc('second', max(this.dwh_loaded_at)), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this)
    {% endif %}
),
insert_data as (
    select
        nextval('dds.seq_rate_sk')::bigint as rate_sk
        , ods_data.rate_bk
        , ods_data.rate
        , ods_data.base
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
        where ods_data.rate_bk = this.rate_bk and ods_data.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.rate_sk
        , this.rate_bk
        , this.rate
        , this.base
        , this.created_at
        , this.updated_at
        , this.dwh_loaded_at
        , this.valid_from
        , {{ current_timestamp() }} as valid_to
        , false as is_current
        , this.hashdiff
    from ods_data
    join {{ this }} this on ods_data.rate_bk = this.rate_bk and ods_data.hashdiff != this.hashdiff and this.is_current
)
{% endif %}
select 
    rate_sk
    , rate_bk
    , rate
    , base
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
    rate_sk
    , rate_bk
    , rate
    , base
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from update_data
{% endif %}