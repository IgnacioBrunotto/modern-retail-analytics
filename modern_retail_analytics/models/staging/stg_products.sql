WITH source AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

deduped AS (
    SELECT
        stock_code,
        product_description,
        unit_price,
        stock_code_type
    FROM source
    WHERE product_description IS NOT NULL
        AND stock_code_type NOT IN ('adjustment', 'fee')
        AND stock_code NOT IN ('TEST001', 'TEST002', 'S', 'PADS')
        AND LOWER(product_description) != 'ebay'
        AND unit_price > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stock_code
        ORDER BY invoice_at DESC
    ) = 1
)

SELECT * FROM deduped