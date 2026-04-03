WITH products AS (
    SELECT * FROM {{ ref('int_products_enriched') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stock_code']) }}  AS product_id,
        stock_code,
        product_description,
        stock_code_type,

        -- Precio --
        unit_price,
        CASE
            WHEN unit_price < 2.50   THEN 'budget'
            WHEN unit_price < 10.00  THEN 'mid'
            ELSE                          'premium'
        END                                                     AS price_tier,

        -- Historial de ventas --
        first_sold_at,
        last_sold_at,
        total_units_sold,
        total_revenue,
        total_orders,
        avg_quantity_per_order,
        days_since_last_sale,
        is_active

    FROM products
)

SELECT * FROM final
