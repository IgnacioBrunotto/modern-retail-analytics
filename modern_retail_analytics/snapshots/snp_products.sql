{% snapshot snp_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='stock_code',
        strategy='check',
        check_cols=['unit_price', 'product_description'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    stock_code,
    product_description,
    unit_price,
    stock_code_type
FROM {{ ref('stg_products') }}

{% endsnapshot %}
