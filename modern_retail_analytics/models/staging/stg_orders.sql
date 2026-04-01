WITH source AS (
    SELECT * FROM {{ source('raw_retail','raw_invoices')}}
),

renamed AS (

        SELECT
            -- IDs --
            CAST(Invoice AS STRING) AS invoice_id,
            CAST(StockCode AS STRING) AS stock_code,
            CAST(`Customer ID` AS STRING) AS customer_id,

            -- Dimensiones --
            INITCAP(TRIM(Description)) AS product_description,
            TRIM(Country) AS country,

            -- Metricas --
            CAST(Quantity AS INT64) AS quantity,
            CAST(Price AS NUMERIC) AS unit_price,

            -- Fechas --
            CAST(InvoiceDate AS TIMESTAMP) AS invoice_at,

            -- Flags --
            CASE
                WHEN CAST(StockCode AS STRING) IN ('ADJUST', 'ADJUST2', 'B', 'M', 'm') THEN 'adjustment'
                WHEN CAST(StockCode AS STRING) IN ('POST', 'DOT', 'C2', 'BANK CHARGES','AMAZONFEE', 'D', 'CRUK') THEN 'fee'
                WHEN REGEXP_CONTAINS(CAST(StockCode AS STRING), r'^\d') THEN 'product'
                ELSE 'other'
            END AS stock_code_type,

            CASE
                WHEN STARTS_WITH(CAST(Invoice AS STRING), 'C') THEN TRUE
                ELSE FALSE
            END                     AS is_cancelled
        
        FROM source
)

SELECT * FROM renamed

