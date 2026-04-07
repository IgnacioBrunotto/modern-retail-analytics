-- Test: no sale line should have negative line_revenue.
-- Returns rows that violate this assertion (dbt fails if any rows returned).
-- Returns and cancellations are intentionally excluded via quantity_type filter.

SELECT
    order_line_id,
    invoice_id,
    stock_code,
    quantity,
    unit_price,
    line_revenue,
    quantity_type
FROM {{ ref('fct_orders') }}
WHERE quantity_type = 'sale'
    AND line_revenue < 0
