{{ config(materialized='incremental')    }}

{%- set source_model = "v_stg_flights__aircrafts"   -%}
{%- set src_pk = "AIRCRAFT_HK"          -%}
{%- set src_nk = "aircraft_code"          -%}
{%- set src_ldts = "LOAD_DATE"      -%}
{%- set src_source = "RECORD_SOURCE"    -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}