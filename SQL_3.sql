# Customer Lifetime Value with Running Totals

%sql
select customer_id, first_name, last_name, order_year, yearly_spend, sum(yearly_spend) over (partition by customer_id order_year rows between unbounded preceding and current row) as total_lifetime_value,
Lag(yearly_spend) over (partition by customer_id order by order_year) as prev_year_spend,
(yearly_spend - lag(yearly_spend) over (partition by customer_id order by order_year))/ Lag(yearly_spend) over (partition by customer_id order by order_year) * 100 as growth_rate from (
select c.customer_id, c.first_time, c.last_time, extract(year from o.order_date) as order_year, sum(o.total_amount) as yearly_spend from customers c join orders o on c.customer_id = o.customer_id group by c.customer_id, c.first_name, c.last_name, extract(year from o.order_date));

# Products with Best/Worst Profit Margins and Reviews

%sql
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.cost,
    AVG(cr.rating) as avg_rating,
    COUNT(cr.review_id) as review_count,
    SUM(oi.quantity * (oi.unit_price - p.cost)) as total_profit,
    AVG(oi.unit_price - p.cost) as avg_profit_margin,
    SUM(oi.quantity) as total_quantity_sold,
    (AVG(oi.unit_price - p.cost) / p.price) * 100 as profit_margin_percentage,
    CASE 
        WHEN AVG(cr.rating) >= 4.5 AND (AVG(oi.unit_price - p.cost) / p.price) * 100 > 30 
            THEN 'High Performer'
        WHEN AVG(cr.rating) <= 2.5 AND (AVG(oi.unit_price - p.cost) / p.price) * 100 < 10 
            THEN 'Low Performer'
        ELSE 'Average'
    END as performance_category
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN customer_reviews cr ON p.product_id = cr.product_id
GROUP BY p.product_id, p.product_name, p.category, p.price, p.cost;

# Customer Segmentation using RFM Analysis

%sql
WITH rfm_base AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        MAX(o.order_date) as last_order_date,
        COUNT(o.order_id) as frequency,
        SUM(o.total_amount) as monetary
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status = 'Delivered'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city
)
SELECT 
    customer_id,
    first_name,
    last_name,
    city,
    last_order_date,
    frequency,
    monetary,
    SYSDATE - last_order_date as recency,
    CASE 
        WHEN SYSDATE - last_order_date <= 30 THEN 5
        WHEN SYSDATE - last_order_date <= 60 THEN 4
        WHEN SYSDATE - last_order_date <= 90 THEN 3
        WHEN SYSDATE - last_order_date <= 180 THEN 2
        ELSE 1
    END as r_score,
    CASE 
        WHEN frequency >= 20 THEN 5
        WHEN frequency >= 10 THEN 4
        WHEN frequency >= 5 THEN 3
        WHEN frequency >= 2 THEN 2
        ELSE 1
    END as f_score,
    CASE 
        WHEN monetary >= 5000 THEN 5
        WHEN monetary >= 2000 THEN 4
        WHEN monetary >= 1000 THEN 3
        WHEN monetary >= 500 THEN 2
        ELSE 1
    END as m_score,
    (CASE WHEN SYSDATE - last_order_date <= 30 THEN 5
        WHEN SYSDATE - last_order_date <= 60 THEN 4
        WHEN SYSDATE - last_order_date <= 90 THEN 3
        WHEN SYSDATE - last_order_date <= 180 THEN 2
        ELSE 1 END +
     CASE WHEN frequency >= 20 THEN 5
        WHEN frequency >= 10 THEN 4
        WHEN frequency >= 5 THEN 3
        WHEN frequency >= 2 THEN 2
        ELSE 1 END +
     CASE WHEN monetary >= 5000 THEN 5
        WHEN monetary >= 2000 THEN 4
        WHEN monetary >= 1000 THEN 3
        WHEN monetary >= 500 THEN 2
        ELSE 1 END) as rfm_score,
    CASE 
        WHEN (CASE WHEN SYSDATE - last_order_date <= 30 THEN 5
              WHEN SYSDATE - last_order_date <= 60 THEN 4
              WHEN SYSDATE - last_order_date <= 90 THEN 3
              WHEN SYSDATE - last_order_date <= 180 THEN 2
              ELSE 1 END +
             CASE WHEN frequency >= 20 THEN 5
              WHEN frequency >= 10 THEN 4
              WHEN frequency >= 5 THEN 3
              WHEN frequency >= 2 THEN 2
              ELSE 1 END +
             CASE WHEN monetary >= 5000 THEN 5
              WHEN monetary >= 2000 THEN 4
              WHEN monetary >= 1000 THEN 3
              WHEN monetary >= 500 THEN 2
              ELSE 1 END) >= 12 THEN 'Champion'
        WHEN (CASE WHEN SYSDATE - last_order_date <= 30 THEN 5
              WHEN SYSDATE - last_order_date <= 60 THEN 4
              WHEN SYSDATE - last_order_date <= 90 THEN 3
              WHEN SYSDATE - last_order_date <= 180 THEN 2
              ELSE 1 END +
             CASE WHEN frequency >= 20 THEN 5
              WHEN frequency >= 10 THEN 4
              WHEN frequency >= 5 THEN 3
              WHEN frequency >= 2 THEN 2
              ELSE 1 END +
             CASE WHEN monetary >= 5000 THEN 5
              WHEN monetary >= 2000 THEN 4
              WHEN monetary >= 1000 THEN 3
              WHEN monetary >= 500 THEN 2
              ELSE 1 END) >= 9 THEN 'Loyal'
        WHEN (CASE WHEN SYSDATE - last_order_date <= 30 THEN 5
              WHEN SYSDATE - last_order_date <= 60 THEN 4
              WHEN SYSDATE - last_order_date <= 90 THEN 3
              WHEN SYSDATE - last_order_date <= 180 THEN 2
              ELSE 1 END +
             CASE WHEN frequency >= 20 THEN 5
              WHEN frequency >= 10 THEN 4
              WHEN frequency >= 5 THEN 3
              WHEN frequency >= 2 THEN 2
              ELSE 1 END +
             CASE WHEN monetary >= 5000 THEN 5
              WHEN monetary >= 2000 THEN 4
              WHEN monetary >= 1000 THEN 3
              WHEN monetary >= 500 THEN 2
              ELSE 1 END) >= 6 THEN 'Potential'
        WHEN (CASE WHEN SYSDATE - last_order_date <= 30 THEN 5
              WHEN SYSDATE - last_order_date <= 60 THEN 4
              WHEN SYSDATE - last_order_date <= 90 THEN 3
              WHEN SYSDATE - last_order_date <= 180 THEN 2
              ELSE 1 END +
             CASE WHEN frequency >= 20 THEN 5
              WHEN frequency >= 10 THEN 4
              WHEN frequency >= 5 THEN 3
              WHEN frequency >= 2 THEN 2
              ELSE 1 END +
             CASE WHEN monetary >= 5000 THEN 5
              WHEN monetary >= 2000 THEN 4
              WHEN monetary >= 1000 THEN 3
              WHEN monetary >= 500 THEN 2
              ELSE 1 END) >= 4 THEN 'At Risk'
        ELSE 'Lost'
    END as customer_segment
FROM rfm_base;

# Monthly Sales Growth with Moving Averages

%sql

WITH monthly_sales AS (
    SELECT 
        TO_CHAR(o.order_date, 'YYYY-MM') as order_month,
        SUM(o.total_amount) as monthly_sales,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        SUM(oi.quantity) as total_units
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY TO_CHAR(o.order_date, 'YYYY-MM')
)
SELECT 
    order_month,
    monthly_sales,
    unique_customers,
    total_units,
    LAG(monthly_sales) OVER (ORDER BY order_month) as prev_month_sales,
    (monthly_sales - LAG(monthly_sales) OVER (ORDER BY order_month)) 
    / LAG(monthly_sales) OVER (ORDER BY order_month) * 100 as sales_growth,
    AVG(monthly_sales) OVER (ORDER BY order_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as three_month_ma,
    AVG(monthly_sales) OVER (ORDER BY order_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as six_month_ma FROM monthly_sales ORDER BY order_month;

# Products Frequently Bought Together (Market Basket Analysis)

%sql
SELECT 
    p1.product_name as product_1,
    p2.product_name as product_2,
    COUNT(*) as pair_count,
    p1.category as category_1,
    p2.category as category_2
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
WHERE oi1.product_id < oi2.product_id
GROUP BY p1.product_name, p2.product_name, p1.category, p2.category
ORDER BY pair_count DESC;

# Customer Churn Prediction Analysis

%sql
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.registration_date,
    c.city,
    MAX(o.order_date) as last_order_date,
    COUNT(o.order_id) as total_orders,
    NVL(SUM(o.total_amount), 0) as total_spent,
    NVL(AVG(o.total_amount), 0) as avg_order_value,
    SYSDATE - MAX(o.order_date) as days_since_last_order,
    SYSDATE - c.registration_date as days_since_registration,
    (SYSDATE - c.registration_date) / GREATEST(COUNT(o.order_id), 1) as order_frequency,
    CASE 
        WHEN (SYSDATE - MAX(o.order_date) > 90 AND COUNT(o.order_id) > 0) THEN 'High'
        WHEN (SYSDATE - MAX(o.order_date) > 60 AND COUNT(o.order_id) > 0) THEN 'Medium'
        WHEN COUNT(o.order_id) = 0 THEN 'New/Inactive'
        ELSE 'Low'
    END as churn_risk,
    CASE 
        WHEN NVL(SUM(o.total_amount), 0) >= 5000 THEN 'Platinum'
        WHEN NVL(SUM(o.total_amount), 0) >= 2000 THEN 'Gold'
        WHEN NVL(SUM(o.total_amount), 0) >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_value_tier
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.registration_date, c.city;

# Hierarchical Supplier Performance Analysis

%sql
WITH supplier_stats AS (
    SELECT 
        s.supplier_id,
        s.supplier_name,
        s.country,
        COUNT(DISTINCT p.product_id) as products_supplied,
        SUM(oi.quantity * (oi.unit_price - p.cost)) as total_profit_generated,
        AVG(s.reliability_score) as avg_reliability,
        SUM(oi.quantity) as total_units_sold,
        AVG(oi.unit_price - p.cost) as avg_profit_margin,
        SUM(oi.quantity * (oi.unit_price - p.cost)) / COUNT(DISTINCT p.product_id) as profit_per_product
    FROM suppliers s
    JOIN products p ON s.supplier_id = p.supplier_id
    JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY s.supplier_id, s.supplier_name, s.country
),
percentiles AS (
    SELECT 
        supplier_id,
        (total_profit_generated * 0.4 + avg_reliability * 100 * 0.3 + profit_per_product * 0.3) as performance_score,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY 
            (total_profit_generated * 0.4 + avg_reliability * 100 * 0.3 + profit_per_product * 0.3)
        ) OVER () as p80,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY 
            (total_profit_generated * 0.4 + avg_reliability * 100 * 0.3 + profit_per_product * 0.3)
        ) OVER () as p60,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY 
            (total_profit_generated * 0.4 + avg_reliability * 100 * 0.3 + profit_per_product * 0.3)
        ) OVER () as p40,
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY 
            (total_profit_generated * 0.4 + avg_reliability * 100 * 0.3 + profit_per_product * 0.3)
        ) OVER () as p20
    FROM supplier_stats
)
SELECT 
    ss.*,
    pt.performance_score,
    CASE 
        WHEN pt.performance_score >= pt.p80 THEN 'A'
        WHEN pt.performance_score >= pt.p60 THEN 'B'
        WHEN pt.performance_score >= pt.p40 THEN 'C'
        WHEN pt.performance_score >= pt.p20 THEN 'D'
        ELSE 'E'
    END as supplier_tier
FROM supplier_stats ss
JOIN percentiles pt ON ss.supplier_id = pt.supplier_id
ORDER BY pt.performance_score DESC;

# Time-based Cohort Analysis

%sql
WITH cohort_data AS (
    SELECT 
        c.customer_id,
        TO_CHAR(c.registration_date, 'YYYY-MM') as cohort_month,
        TO_CHAR(o.order_date, 'YYYY-MM') as order_month,
        MONTHS_BETWEEN(TO_DATE(TO_CHAR(o.order_date, 'YYYY-MM'), 'YYYY-MM'), 
                      TO_DATE(TO_CHAR(c.registration_date, 'YYYY-MM'), 'YYYY-MM')) as cohort_index
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= c.registration_date
),
cohort_counts AS (
    SELECT 
        cohort_month,
        cohort_index,
        COUNT(DISTINCT customer_id) as customers
    FROM cohort_data
    WHERE cohort_index >= 0 AND cohort_index <= 12
    GROUP BY cohort_month, cohort_index
),
cohort_pivot AS (
    SELECT *
    FROM cohort_counts
    PIVOT (
        MAX(customers)
        FOR cohort_index IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)))
SELECT 
    cohort_month,
    "0" as month_0,
    ROUND(NVL("1", 0) / "0" * 100, 2) as month_1_retention,
    ROUND(NVL("2", 0) / "0" * 100, 2) as month_2_retention,
    ROUND(NVL("3", 0) / "0" * 100, 2) as month_3_retention,
    ROUND(NVL("6", 0) / "0" * 100, 2) as month_6_retention,
    ROUND(NVL("12", 0) / "0" * 100, 2) as month_12_retention FROM cohort_pivot ORDER BY cohort_month;

# Advanced Product Recommendation Engine

%sql
WITH user_product_ratings AS (
    SELECT 
        o.customer_id,
        oi.product_id,
        COALESCE(AVG(cr.rating), 3.0) as implicit_rating,
        COUNT(o.order_id) as purchase_count,
        SUM(oi.quantity) as total_quantity,
        COALESCE(AVG(cr.rating), 3.0) * LN(COUNT(o.order_id) + 1) as weighted_rating
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN customer_reviews cr ON o.customer_id = cr.customer_id AND oi.product_id = cr.product_id
    GROUP BY o.customer_id, oi.product_id
),
product_similarity AS (
    SELECT 
        upr1.product_id as product1,
        upr2.product_id as product2,
        SUM(upr1.weighted_rating * upr2.weighted_rating) as dot_product,
        SQRT(SUM(upr1.weighted_rating * upr1.weighted_rating)) as norm1,
        SQRT(SUM(upr2.weighted_rating * upr2.weighted_rating)) as norm2,
        CASE 
            WHEN SQRT(SUM(upr1.weighted_rating * upr1.weighted_rating)) * 
                 SQRT(SUM(upr2.weighted_rating * upr2.weighted_rating)) > 0
            THEN SUM(upr1.weighted_rating * upr2.weighted_rating) / 
                 (SQRT(SUM(upr1.weighted_rating * upr1.weighted_rating)) * 
                  SQRT(SUM(upr2.weighted_rating * upr2.weighted_rating)))
            ELSE 0
        END as cosine_similarity
    FROM user_product_ratings upr1
    JOIN user_product_ratings upr2 ON upr1.customer_id = upr2.customer_id
    WHERE upr1.product_id != upr2.product_id
    GROUP BY upr1.product_id, upr2.product_id)
SELECT 
    p1.product_name as product,
    p2.product_name as similar_product,
    ps.cosine_similarity FROM product_similarity ps
JOIN products p1 ON ps.product1 = p1.product_id
JOIN products p2 ON ps.product2 = p2.product_id WHERE ps.cosine_similarity > 0.5 ORDER BY ps.cosine_similarity DESC;

# Geographic Sales Analysis with Advanced Analytics

%sql
WITH geographic_analysis AS (
    SELECT 
        c.country,
        c.city,
        p.category,
        SUM(o.total_amount) as total_sales,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        SUM(oi.quantity) as units_sold,
        AVG(o.total_amount) as avg_order_value,
        SUM(oi.quantity * (oi.unit_price - p.cost)) as total_profit,
        SUM(o.total_amount) / COUNT(DISTINCT o.customer_id) as sales_per_customer,
        (SUM(oi.quantity * (oi.unit_price - p.cost)) / SUM(o.total_amount)) * 100 as profit_margin
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY c.country, c.city, p.category
),
geo_concentration AS (
    SELECT 
        country,
        SUM(total_sales) as country_sales,
        COUNT(DISTINCT city) as cities_count,
        SUM(unique_customers) as total_customers,
        PERCENT_RANK() OVER (ORDER BY SUM(total_sales) DESC) as sales_concentration,
        PERCENT_RANK() OVER (ORDER BY SUM(unique_customers) DESC) as customer_concentration
    FROM geographic_analysis
    GROUP BY country
),
top_cities AS (
    SELECT 
        country,
        city,
        category,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY country, category ORDER BY total_sales DESC) as rank FROM geographic_analysis)
SELECT 
    country,
    city,
    category,
    total_sales,
    rank FROM top_cities WHERE rank <= 3 ORDER BY country, category, rank;

# Customer Journey Analysis

%sql
WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        o.total_amount,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date) as order_sequence,
        LAG(o.order_date) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) as prev_order_date,
        LAG(o.total_amount) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) as prev_order_amount,
        MAX(o.order_date) OVER (PARTITION BY c.customer_id) as last_order_date,
        MIN(o.order_date) OVER (PARTITION BY c.customer_id) as first_order_date
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
),
journey_metrics AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        COUNT(order_id) as total_orders,
        AVG(order_date - prev_order_date) as avg_days_between_orders,
        STDDEV(order_date - prev_order_date) as stddev_days_between_orders,
        AVG(total_amount) as avg_order_value,
        SUM(total_amount) as lifetime_value,
        MAX(order_sequence) as max_order_sequence,
        MAX(order_date) - MIN(order_date) as customer_tenure_days,
        CASE 
            WHEN COUNT(order_id) = 1 THEN 'New'
            WHEN COUNT(order_id) <= 3 THEN 'Developing'
            WHEN COUNT(order_id) <= 10 THEN 'Established'
            ELSE 'Loyal' END as customer_stage, (MAX(order_date) - MIN(order_date)) / GREATEST(COUNT(order_id) - 1, 1) as order_frequency FROM customer_orders GROUP BY customer_id, first_name, last_name)
SELECT * FROM journey_metrics ORDER BY lifetime_value DESC;

# Advanced Inventory Analysis

%sql
WITH inventory_analysis AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        s.supplier_name,
        NVL(SUM(oi.quantity), 0) as total_sold,
        NVL(AVG(oi.unit_price), p.price) as avg_selling_price,
        p.price as current_price,
        p.cost as unit_cost,
        COUNT(DISTINCT oi.order_id) as orders_count,
        NVL(AVG(cr.rating), 0) as avg_rating,
        NVL(SUM(oi.quantity), 0) * NVL(AVG(oi.unit_price), p.price) as total_revenue,
        NVL(SUM(oi.quantity), 0) * p.cost as total_cost,
        NVL(SUM(oi.quantity), 0) * NVL(AVG(oi.unit_price), p.price) - NVL(SUM(oi.quantity), 0) * p.cost as total_profit,
        CASE 
            WHEN NVL(SUM(oi.quantity), 0) * NVL(AVG(oi.unit_price), p.price) > 0 
            THEN (NVL(SUM(oi.quantity), 0) * NVL(AVG(oi.unit_price), p.price) - NVL(SUM(oi.quantity), 0) * p.cost) 
                 / (NVL(SUM(oi.quantity), 0) * NVL(AVG(oi.unit_price), p.price)) * 100
            ELSE 0
        END as profit_margin,
        NVL(SUM(oi.quantity), 0) / GREATEST(COUNT(DISTINCT oi.order_id), 1) as turnover_rate
    FROM products p
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    LEFT JOIN customer_reviews cr ON p.product_id = cr.product_id
    JOIN suppliers s ON p.supplier_id = s.supplier_id
    GROUP BY p.product_id, p.product_name, p.category, s.supplier_name, p.price, p.cost
),
abc_analysis AS (
    SELECT 
        product_id,
        product_name,
        category,
        total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) as cumulative_revenue,
        SUM(total_revenue) OVER () as total_revenue_sum,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / SUM(total_revenue) OVER () * 100 as revenue_percentage,
        CASE 
            WHEN SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / SUM(total_revenue) OVER () * 100 <= 80 THEN 'A'
            WHEN SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / SUM(total_revenue) OVER () * 100 <= 95 THEN 'B'
            ELSE 'C' END as abc_class FROM inventory_analysis)
SELECT * FROM abc_analysis ORDER BY total_revenue DESC;

# Customer Behavior Clustering Preparation

%sql
WITH customer_features AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        c.country,
        c.registration_date,
        COUNT(DISTINCT o.order_id) as order_count,
        NVL(SUM(o.total_amount), 0) as total_spent,
        NVL(AVG(o.total_amount), 0) as avg_order_value,
        NVL(SYSDATE - MAX(o.order_date), SYSDATE - c.registration_date) as days_since_last_order,
        SYSDATE - c.registration_date as days_since_first_order,
        NVL(SUM(oi.quantity), 0) as total_items_purchased,
        NVL(AVG(oi.unit_price - p.cost), 0) as avg_profit_margin,
        COUNT(DISTINCT oi.product_id) as unique_products,
        CASE 
            WHEN SYSDATE - c.registration_date > 0 
            THEN COUNT(DISTINCT o.order_id) / (SYSDATE - c.registration_date) * 30
            ELSE 0
        END as order_frequency,
        NVL(SUM(oi.quantity), 0) / GREATEST(COUNT(DISTINCT o.order_id), 1) as avg_items_per_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products p ON oi.product_id = p.product_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.country, c.registration_date
),
normalized_features AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        (order_count - AVG(order_count) OVER ()) / STDDEV(order_count) OVER () as norm_order_count,
        (total_spent - AVG(total_spent) OVER ()) / STDDEV(total_spent) OVER () as norm_total_spent,
        (avg_order_value - AVG(avg_order_value) OVER ()) / STDDEV(avg_order_value) OVER () as norm_avg_order_value,
        (days_since_last_order - AVG(days_since_last_order) OVER ()) / STDDEV(days_since_last_order) OVER () as norm_days_since_last_order,
        (total_items_purchased - AVG(total_items_purchased) OVER ()) / STDDEV(total_items_purchased) OVER () as norm_total_items,
        (avg_profit_margin - AVG(avg_profit_margin) OVER ()) / STDDEV(avg_profit_margin) OVER () as norm_profit_margin,
        (unique_products - AVG(unique_products) OVER ()) / STDDEV(unique_products) OVER () as norm_unique_products,
        (order_frequency - AVG(order_frequency) OVER ()) / STDDEV(order_frequency) OVER () as norm_order_frequency
    FROM customer_features
)
SELECT 
    customer_id,
    first_name,
    last_name,
    norm_order_count,
    norm_total_spent,
    norm_avg_order_value,
    norm_days_since_last_order,
    norm_total_items,
    norm_profit_margin,
    norm_unique_products,
    norm_order_frequency
FROM normalized_features;

# Advanced Time Series Forecasting Preparation

%sql
WITH time_series_data AS (
    SELECT 
        TRUNC(o.order_date) as order_date,
        p.category,
        SUM(o.total_amount) as daily_sales,
        SUM(oi.quantity) as daily_units,
        COUNT(DISTINCT o.customer_id) as daily_customers,
        AVG(o.total_amount) as avg_order_value,
        SUM(oi.quantity * (oi.unit_price - p.cost)) as daily_profit,
        TO_CHAR(o.order_date, 'D') as day_of_week,
        CASE WHEN TO_CHAR(o.order_date, 'D') IN ('1', '7') THEN 1 ELSE 0 END as is_weekend,
        EXTRACT(MONTH FROM o.order_date) as month,
        EXTRACT(YEAR FROM o.order_date) as year,
        EXTRACT(QUARTER FROM o.order_date) as quarter
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY TRUNC(o.order_date), p.category, TO_CHAR(o.order_date, 'D'), 
             EXTRACT(MONTH FROM o.order_date), EXTRACT(YEAR FROM o.order_date), 
             EXTRACT(QUARTER FROM o.order_date)
),
lagged_features AS (
    SELECT 
        order_date,
        category,
        daily_sales,
        daily_units,
        daily_customers,
        avg_order_value,
        daily_profit,
        day_of_week,
        is_weekend,
        month,
        year,
        quarter,
        LAG(daily_sales) OVER (PARTITION BY category ORDER BY order_date) as prev_day_sales,
        LAG(daily_sales, 7) OVER (PARTITION BY category ORDER BY order_date) as prev_week_sales,
        AVG(daily_sales) OVER (
            PARTITION BY category 
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_avg,
        AVG(daily_sales) OVER (
            PARTITION BY category 
            ORDER BY order_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_avg,
        (daily_sales - LAG(daily_sales) OVER (PARTITION BY category ORDER BY order_date)) 
        / LAG(daily_sales) OVER (PARTITION BY category ORDER BY order_date) * 100 as sales_growth,
        (daily_sales - LAG(daily_sales, 365) OVER (PARTITION BY category ORDER BY order_date)) 
        / LAG(daily_sales, 365) OVER (PARTITION BY category ORDER BY order_date) * 100 as yoy_growth
    FROM time_series_data
),
seasonality_analysis AS (
    SELECT 
        category,
        month,
        day_of_week,
        AVG(daily_sales) as avg_sales_seasonal,
        STDDEV(daily_sales) as std_sales_seasonal,
        COUNT(*) as data_points,
        AVG(daily_sales) / AVG(AVG(daily_sales)) OVER (PARTITION BY category) as seasonal_index
    FROM lagged_features
    WHERE order_date >= ADD_MONTHS(SYSDATE, -24)
    GROUP BY category, month, day_of_week)
SELECT * FROM seasonality_analysis ORDER BY category, month, day_of_week;

# Multi-dimensional Customer Value Scoring

%sql
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        c.country,
        c.registration_date,
        NVL(SUM(o.total_amount), 0) as total_spent,
        NVL(AVG(o.total_amount), 0) as avg_order_value,
        NVL(SUM(oi.quantity * (oi.unit_price - p.cost)), 0) as total_profit_contributed,
        COUNT(DISTINCT o.order_id) as order_count,
        COUNT(DISTINCT oi.product_id) as unique_products,
        NVL(SYSDATE - MAX(o.order_date), SYSDATE - c.registration_date) as days_since_last_order,
        SYSDATE - c.registration_date as days_since_first_order,
        NVL(AVG(cr.rating), 0) as avg_rating_given,
        NVL(SUM(cr.helpful_votes), 0) as total_helpful_votes,
        SYSDATE - c.registration_date as customer_tenure,
        COUNT(DISTINCT o.order_id) / GREATEST((SYSDATE - c.registration_date) / 30, 1) as order_frequency,
        NVL(SUM(oi.quantity * (oi.unit_price - p.cost)), 0) / GREATEST(COUNT(DISTINCT o.order_id), 1) as avg_profit_per_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products p ON oi.product_id = p.product_id
    LEFT JOIN customer_reviews cr ON c.customer_id = cr.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.country, c.registration_date
),
with_z_scores AS (
    SELECT 
        *,
        (total_spent - AVG(total_spent) OVER ()) / STDDEV(total_spent) OVER () as monetary_z,
        (order_frequency - AVG(order_frequency) OVER ()) / STDDEV(order_frequency) OVER () as frequency_z,
        ((-days_since_last_order) - AVG(-days_since_last_order) OVER ()) / STDDEV(-days_since_last_order) OVER () as recency_z,
        (total_spent - AVG(total_spent) OVER ()) / STDDEV(total_spent) OVER () * 0.4 +
        (order_frequency - AVG(order_frequency) OVER ()) / STDDEV(order_frequency) OVER () * 0.3 +
        ((-days_since_last_order) - AVG(-days_since_last_order) OVER ()) / STDDEV(-days_since_last_order) OVER () * 0.3 as composite_score,
        PERCENT_RANK() OVER (ORDER BY total_spent) as monetary_percentile,
        PERCENT_RANK() OVER (ORDER BY order_frequency) as frequency_percentile
    FROM customer_metrics
)
SELECT 
    customer_id,
    first_name,
    last_name,
    city,
    country,
    total_spent,
    order_frequency,
    days_since_last_order,
    composite_score,
    CASE 
        WHEN composite_score >= 1.0 THEN 'VIP'
        WHEN composite_score >= 0.5 THEN 'Premium'
        WHEN composite_score >= -0.5 THEN 'Standard'
        WHEN composite_score >= -1.0 THEN 'Opportunity'
        ELSE 'At Risk'
    END as value_segment,
    monetary_percentile,
    frequency_percentile,
    PERCENT_RANK() OVER (ORDER BY composite_score) as value_percentile FROM with_z_scores ORDER BY composite_score DESC;

# Cross-sell and Up-sell Opportunity Analysis

%sql
WITH category_affinity AS (
    SELECT 
        o.customer_id,
        p.category,
        COUNT(o.order_id) as order_count,
        SUM(oi.quantity) as total_quantity,
        AVG(oi.unit_price) as avg_spent,
        COUNT(o.order_id) * SUM(oi.quantity) * AVG(oi.unit_price) as affinity_score
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.customer_id, p.category
),
cross_sell_opportunities AS (
    SELECT 
        ca1.customer_id,
        ca1.category as source_category,
        ca2.category as target_category,
        AVG(ca1.affinity_score) as source_affinity,
        AVG(ca2.affinity_score) as target_affinity,
        AVG(ca1.affinity_score) * (1 / (AVG(ca2.affinity_score) + 1)) as opportunity_score
    FROM category_affinity ca1
    JOIN category_affinity ca2 ON ca1.customer_id = ca2.customer_id
    WHERE ca1.category != ca2.category
    GROUP BY ca1.customer_id, ca1.category, ca2.category
)
SELECT 
    cso.customer_id,
    c.first_name,
    c.last_name,
    cso.source_category,
    cso.target_category,
    cso.opportunity_score
FROM cross_sell_opportunities cso
JOIN customers c ON cso.customer_id = c.customer_id
WHERE cso.opportunity_score > 1000  -- Threshold for meaningful opportunities
ORDER BY cso.opportunity_score DESC;

# Advanced Supplier Risk Assessment

%sql
WITH supplier_metrics AS (
    SELECT 
        s.supplier_id,
        s.supplier_name,
        s.country,
        s.reliability_score,
        COUNT(DISTINCT p.product_id) as products_supplied,
        SUM(oi.quantity) as total_units_sold,
        SUM(oi.quantity * oi.unit_price) as total_revenue,
        AVG(oi.unit_price - p.cost) as avg_profit_margin,
        STDDEV(oi.unit_price - p.cost) as margin_volatility,
        MAX(o.order_date) - MIN(o.order_date) as supplier_activity_period,
        AVG(o.order_date - LAG(o.order_date) OVER (PARTITION BY s.supplier_id ORDER BY o.order_date)) as avg_days_between_orders,
        COUNT(DISTINCT o.customer_id) as customer_base,
        COUNT(DISTINCT c.country) as geographic_diversity
    FROM suppliers s
    JOIN products p ON s.supplier_id = p.supplier_id
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY s.supplier_id, s.supplier_name, s.country, s.reliability_score
),
supplier_risk AS (
    SELECT 
        *,
        total_revenue / SUM(total_revenue) OVER () as revenue_concentration,
        CASE 
            WHEN total_revenue / SUM(total_revenue) OVER () > 0.1 THEN 'High'
            WHEN total_revenue / SUM(total_revenue) OVER () > 0.05 THEN 'Medium'
            ELSE 'Low'
        END as dependency_risk,
        CASE 
            WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.5 THEN 'High Volatility'
            WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.2 THEN 'Medium Volatility'
            ELSE 'Stable'
        END as margin_stability,
        CASE 
            WHEN supplier_activity_period < 90 THEN 'New Supplier'
            WHEN avg_days_between_orders > 30 THEN 'Intermittent'
            ELSE 'Regular'
        END as business_continuity_risk,
        (CASE 
            WHEN total_revenue / SUM(total_revenue) OVER () > 0.1 THEN 3
            WHEN total_revenue / SUM(total_revenue) OVER () > 0.05 THEN 2
            ELSE 1
         END +
         CASE 
            WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.5 THEN 3
            WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.2 THEN 2
            ELSE 1
         END +
         CASE 
            WHEN supplier_activity_period < 90 THEN 3
            WHEN avg_days_between_orders > 30 THEN 2
            ELSE 1
         END) as overall_risk_score,
        CASE 
            WHEN (CASE 
                    WHEN total_revenue / SUM(total_revenue) OVER () > 0.1 THEN 3
                    WHEN total_revenue / SUM(total_revenue) OVER () > 0.05 THEN 2
                    ELSE 1
                 END +
                 CASE 
                    WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.5 THEN 3
                    WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.2 THEN 2
                    ELSE 1
                 END +
                 CASE 
                    WHEN supplier_activity_period < 90 THEN 3
                    WHEN avg_days_between_orders > 30 THEN 2
                    ELSE 1
                 END) >= 7 THEN 'High Risk'
            WHEN (CASE 
                    WHEN total_revenue / SUM(total_revenue) OVER () > 0.1 THEN 3
                    WHEN total_revenue / SUM(total_revenue) OVER () > 0.05 THEN 2
                    ELSE 1
                 END +
                 CASE 
                    WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.5 THEN 3
                    WHEN margin_volatility / GREATEST(avg_profit_margin, 0.01) > 0.2 THEN 2
                    ELSE 1
                 END +
                 CASE 
                    WHEN supplier_activity_period < 90 THEN 3
                    WHEN avg_days_between_orders > 30 THEN 2
                    ELSE 1
                 END) >= 5 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END as risk_category
    FROM supplier_metrics
)
SELECT * FROM supplier_risk
ORDER BY overall_risk_score DESC;

# Customer Sentiment and Review Analysis

%sql
WITH sentiment_analysis AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        AVG(cr.rating) as avg_rating,
        COUNT(cr.review_id) as review_count,
        SUM(cr.helpful_votes) as total_helpful_votes,
        SUM(CASE WHEN cr.rating = 5 THEN 1 ELSE 0 END) as five_star_reviews,
        SUM(CASE WHEN cr.rating = 4 THEN 1 ELSE 0 END) as four_star_reviews,
        SUM(CASE WHEN cr.rating = 3 THEN 1 ELSE 0 END) as three_star_reviews,
        SUM(CASE WHEN cr.rating = 2 THEN 1 ELSE 0 END) as two_star_reviews,
        SUM(CASE WHEN cr.rating = 1 THEN 1 ELSE 0 END) as one_star_reviews,
        SYSDATE - MAX(cr.review_date) as days_since_last_review,
        MAX(cr.review_date) - MIN(cr.review_date) as review_period_days,
        SUM(cr.helpful_votes) / GREATEST(COUNT(cr.review_id), 1) as helpfulness_ratio,
        (SUM(CASE WHEN cr.rating = 5 THEN 1 ELSE 0 END) + SUM(CASE WHEN cr.rating = 4 THEN 1 ELSE 0 END)) 
        / COUNT(cr.review_id) as positive_review_ratio,
        (SUM(CASE WHEN cr.rating = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN cr.rating = 2 THEN 1 ELSE 0 END)) 
        / COUNT(cr.review_id) as negative_review_ratio,
        COUNT(cr.review_id) / GREATEST((MAX(cr.review_date) - MIN(cr.review_date)) / 30, 1) as review_velocity,
        (AVG(cr.rating) * 0.4 +
         (SUM(CASE WHEN cr.rating = 5 THEN 1 ELSE 0 END) + SUM(CASE WHEN cr.rating = 4 THEN 1 ELSE 0 END)) 
         / COUNT(cr.review_id) * 0.3 +
         SUM(cr.helpful_votes) / GREATEST(COUNT(cr.review_id), 1) * 0.2 +
         COUNT(cr.review_id) / GREATEST((MAX(cr.review_date) - MIN(cr.review_date)) / 30, 1) * 0.1) as sentiment_score
    FROM customer_reviews cr
    JOIN products p ON cr.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.category
)
SELECT 
    product_id,
    product_name,
    category,
    avg_rating,
    review_count,
    total_helpful_votes,
    helpfulness_ratio,
    positive_review_ratio,
    negative_review_ratio,
    review_velocity,
    sentiment_score,
    CASE 
        WHEN sentiment_score >= 4.5 THEN 'Excellent'
        WHEN sentiment_score >= 4.0 THEN 'Very Good'
        WHEN sentiment_score >= 3.5 THEN 'Good'
        WHEN sentiment_score >= 3.0 THEN 'Average'
        WHEN sentiment_score >= 2.5 THEN 'Below Average'
        ELSE 'Poor'
    END as sentiment_category
FROM sentiment_analysis
ORDER BY sentiment_score DESC;

# Comprehensive Business Health Dashboard

%sql
WITH business_health AS (
    SELECT 
        EXTRACT(YEAR FROM o.order_date) as order_year,
        TO_CHAR(o.order_date, 'YYYY-MM') as order_month,
        SUM(o.total_amount) as monthly_revenue,
        COUNT(DISTINCT o.order_id) as order_count,
        COUNT(DISTINCT o.customer_id) as active_customers,
        COUNT(DISTINCT CASE WHEN o.order_date = first_order.first_order_date THEN o.customer_id END) as new_customers,
        AVG(o.total_amount) as avg_order_value,
        COUNT(DISTINCT oi.product_id) as unique_products_sold,
        SUM(oi.quantity) as total_units_sold,
        SUM(oi.quantity * (oi.unit_price - p.cost)) as gross_profit,
        AVG(oi.unit_price - p.cost) as avg_profit_margin,
        COUNT(DISTINCT p.supplier_id) as active_suppliers,
        AVG(s.reliability_score) as avg_supplier_reliability,
        (SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM'))) 
        / LAG(SUM(o.total_amount)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM')) * 100 as revenue_growth,
        (COUNT(DISTINCT o.customer_id) - LAG(COUNT(DISTINCT o.customer_id)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM'))) 
        / LAG(COUNT(DISTINCT o.customer_id)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM')) * 100 as customer_growth,
        (SUM(oi.quantity * (oi.unit_price - p.cost)) / SUM(o.total_amount)) * 100 as profit_margin_percentage,
        ((SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM'))) 
        / LAG(SUM(o.total_amount)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM')) * 100 * 0.25 +
        (COUNT(DISTINCT o.customer_id) - LAG(COUNT(DISTINCT o.customer_id)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM'))) 
        / LAG(COUNT(DISTINCT o.customer_id)) OVER (ORDER BY TO_CHAR(o.order_date, 'YYYY-MM')) * 100 * 0.25 +
        (SUM(oi.quantity * (oi.unit_price - p.cost)) / SUM(o.total_amount)) * 100 * 0.3 +
        AVG(s.reliability_score) * 0.2) as health_score
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN suppliers s ON p.supplier_id = s.supplier_id
    JOIN (
        SELECT customer_id, MIN(order_date) as first_order_date
        FROM orders
        GROUP BY customer_id
    ) first_order ON o.customer_id = first_order.customer_id
    GROUP BY EXTRACT(YEAR FROM o.order_date), TO_CHAR(o.order_date, 'YYYY-MM')
)
SELECT 
    order_year,
    order_month,
    monthly_revenue,
    order_count,
    active_customers,
    new_customers,
    avg_order_value,
    unique_products_sold,
    total_units_sold,
    gross_profit,
    avg_profit_margin,
    active_suppliers,
    avg_supplier_reliability,
    revenue_growth,
    customer_growth,
    profit_margin_percentage,
    health_score,
    CASE 
        WHEN health_score >= 20 THEN 'Excellent'
        WHEN health_score >= 10 THEN 'Good'
        WHEN health_score >= 0 THEN 'Stable'
        WHEN health_score >= -10 THEN 'Concerning'
        ELSE 'Critical' END as business_health FROM business_health ORDER BY order_year DESC, order_month DESC;