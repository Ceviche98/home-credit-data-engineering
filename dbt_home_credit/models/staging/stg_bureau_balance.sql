{{ config(materialized='view') }}

SELECT
    SK_ID_CURR,
    SK_ID_BUREAU,
    MONTHS_BALANCE,
    STATUS
FROM {{ source('silver', 'bureau_balance') }}
