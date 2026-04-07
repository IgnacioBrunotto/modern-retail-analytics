{% snapshot snp_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['country'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    country
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
