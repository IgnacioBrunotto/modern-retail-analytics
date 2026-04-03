WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

country_regions AS (
    SELECT * FROM {{ ref('country_regions') }}
),

joined AS (
    SELECT
        orders.*,
        country_regions.region
    FROM orders
    LEFT JOIN country_regions
        ON orders.country = country_regions.country
),

aggregated AS (
    SELECT
        DATE_TRUNC(DATE(invoice_at), MONTH)         AS month,
        country,
        region,

        -- Volumen --
        COUNT(DISTINCT invoice_id)                  AS total_orders,
        COUNT(DISTINCT customer_id)                 AS total_customers,

        -- Revenue --
        SUM(CASE WHEN quantity_type = 'sale'
            THEN line_revenue ELSE 0 END)           AS revenue_gross,
        SUM(CASE WHEN quantity_type = 'cancellation'
            THEN line_revenue ELSE 0 END)           AS revenue_cancelled,
        SUM(line_revenue)                           AS revenue_net,

        -- AOV --
        SUM(CASE WHEN quantity_type = 'sale'
            THEN line_revenue ELSE 0 END)
            / NULLIF(COUNT(DISTINCT CASE WHEN quantity_type = 'sale'
            THEN invoice_id END), 0)                AS avg_order_value

    FROM joined
    GROUP BY DATE_TRUNC(DATE(invoice_at), MONTH), country, region
)

SELECT * FROM aggregated
