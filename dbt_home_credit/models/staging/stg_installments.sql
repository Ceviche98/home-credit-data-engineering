{{ config(materialized='view') }}

SELECT
    SK_ID_PREV,
    SK_ID_CURR,
    NUM_INSTALMENT_VERSION,
    NUM_INSTALMENT_NUMBER,
    DAYS_INSTALMENT,
    DAYS_ENTRY_PAYMENT,
    AMT_INSTALMENT,
    AMT_PAYMENT,
    DAYS_LATE,
    AMT_UNDERPAYMENT
FROM {{ source('silver', 'installments_payments') }}
