WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

history AS (
    SELECT * FROM {{ ref('int_customer_order_history') }}
),

country_regions AS (
    SELECT * FROM {{ ref('country_regions') }}
),

dataset_max_date AS (
    SELECT DATE(MAX(last_order_at)) AS max_date
    FROM history
),

final AS (
    SELECT
        -- Surrogate key --
        {{ dbt_utils.generate_surrogate_key(['customers.customer_id']) }} AS customer_id_key,

        -- IDs --
        customers.customer_id,
        customers.country,
        country_regions.region,

        -- Historial de compras --
        history.first_order_at,
        history.last_order_at,
        history.total_orders,
        history.total_line_items,
        history.total_unique_products,
        history.customer_lifespan_days,
        history.is_repeat_customer,

        -- Revenue --
        history.total_revenue_gross,
        history.total_revenue_net,
        history.total_revenue_cancelled,
        history.avg_order_value,

        -- Atributos derivados --
        DATE_DIFF(d.max_date, DATE(history.last_order_at), DAY)     AS days_since_last_order,

        CASE
            WHEN history.total_revenue_gross >= 5500    THEN 'platinum'
            WHEN history.total_revenue_gross >= 2500    THEN 'gold'
            WHEN history.total_revenue_gross >= 500     THEN 'silver'
            ELSE                                             'bronze'
        END                                                         AS customer_tier

    FROM customers
    LEFT JOIN history
        ON customers.customer_id = history.customer_id
    LEFT JOIN country_regions
        ON customers.country = country_regions.country
    CROSS JOIN dataset_max_date AS d
)

SELECT * FROM final
