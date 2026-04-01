WITH source AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

deduped AS (
    SELECT
        customer_id,
        country
    FROM source
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY invoice_at DESC
    ) = 1
)

SELECT * FROM deduped