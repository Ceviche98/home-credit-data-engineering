{{ config(materialized='view') }}

SELECT
    SK_ID_CURR,
    SK_ID_PREV,
    MONTHS_BALANCE,
    CNT_INSTALMENT,
    CNT_INSTALMENT_FUTURE,
    NAME_CONTRACT_STATUS,
    SK_DPD,
    SK_DPD_DEF
FROM {{ source('silver', 'pos_cash_balance') }}
