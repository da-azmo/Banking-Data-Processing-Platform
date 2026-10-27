{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , schema='dds'
    , unique_key='card_sk'
    , on_schema_change='fail') }}

with ods_data as (
    select
        id as card_bk
        , status
        , account_id as account_bk
        , number
        , term
        , created_at
        , updated_at
        , dwh_loaded_at
        , {{ current_timestamp() }} as valid_from
        , '2999-01-01 00:00:00'::timestamptz as valid_to
        , true as is_current
        , MD5(coalesce(id::text, '')  || '|' || 
            coalesce(status::text, '') || '|' || 
            coalesce(account_id::text, '') || '|' || 
            coalesce(number::text, '') || '|' || 
            coalesce(term::text, '')) as hashdiff
    from {{ ref("cards") }}

    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(max(this.updated_at), '1900-01-01 00:00:00'::timestamptz)
        from {{ this }} this
    )
    {% endif %}
),
data_with_fk as (
    select
        ods_data.card_bk
        , ods_data.status
        , bank_accounts_scd2.account_sk as account_sk
        , ods_data.number
        , ods_data.term
        , ods_data.created_at
        , ods_data.updated_at
        , ods_data.dwh_loaded_at
        , ods_data.valid_from
        , ods_data.valid_to
        , ods_data.is_current
        , ods_data.hashdiff
    from ods_data
    join {{ ref('bank_accounts_scd2') }} bank_accounts_scd2 on
        bank_accounts_scd2.account_bk = ods_data.account_bk and bank_accounts_scd2.is_current
),
insert_data as (
    select
        nextval('dds.seq_card_sk')::bigint as card_sk
        , data_with_fk.card_bk
        , data_with_fk.status
        , data_with_fk.account_sk
        , data_with_fk.number
        , data_with_fk.term
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
        where data_with_fk.card_bk = this.card_bk and data_with_fk.hashdiff = this.hashdiff and this.is_current
    )
    {% endif %}
),
{% if is_incremental() %}
update_data as (
    select
        this.card_sk
        , this.card_bk
        , this.status
        , this.account_sk
        , this.number
        , this.term
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
    card_sk
    , card_bk
    , status
    , account_sk
    , number
    , term
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
    card_sk
    , card_bk
    , status
    , account_sk
    , number
    , term
    , created_at
    , updated_at
    , valid_from
    , valid_to
    , is_current
    , dwh_loaded_at
    , hashdiff
from update_data
{% endif %}