--- tables

-- 1. Users table (prerequisite for journeys)
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Infrastructure Nodes (prerequisite for journey legs)
CREATE TABLE infrastructure_nodes (
    node_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_type node_type_enum NOT NULL,
    name VARCHAR(255) NOT NULL,
    location GEOGRAPHY(Point, 4326) NOT NULL,
    address TEXT,
    operating_hours JSONB,
    properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_operating_hours CHECK (
        operating_hours IS NULL OR jsonb_typeof(operating_hours) = 'object'
    )
);

-- 3. Transport Assets
CREATE TABLE transport_assets (
    asset_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_type transport_mode_enum NOT NULL,
    identifier VARCHAR(50) UNIQUE NOT NULL,
    model VARCHAR(100),
    manufacturer VARCHAR(100),
    year_of_manufacture INTEGER,
    capacity INTEGER CHECK (capacity > 0),
    current_status asset_status_enum DEFAULT 'active',
    last_known_location GEOGRAPHY(Point, 4326),
    last_updated_location TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_manufacture_year CHECK (
        year_of_manufacture IS NULL OR 
        year_of_manufacture BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1
    )
);

-- 4. Multi Modal Journey
CREATE TABLE multi_modal_journey (
    journey_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    planned_departure_time TIMESTAMP NOT NULL,
    planned_arrival_time TIMESTAMP NOT NULL,
    actual_departure_time TIMESTAMP,
    actual_arrival_time TIMESTAMP,
    total_cost DECIMAL(10,2) DEFAULT 0.00,
    journey_status journey_status_enum DEFAULT 'planned',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CONSTRAINT valid_timestamps CHECK (
        planned_departure_time < planned_arrival_time AND
        (actual_departure_time IS NULL OR actual_departure_time <= COALESCE(actual_arrival_time, CURRENT_TIMESTAMP))
    ),
    CONSTRAINT non_negative_cost CHECK (total_cost >= 0)
);

-- 5. Journey Leg
CREATE TABLE journey_leg (
    leg_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journey_id UUID NOT NULL,
    leg_sequence SMALLINT NOT NULL,
    transport_mode transport_mode_enum NOT NULL,
    asset_id UUID,
    start_location_id UUID NOT NULL,
    end_location_id UUID NOT NULL,
    scheduled_start_time TIMESTAMP NOT NULL,
    scheduled_end_time TIMESTAMP NOT NULL,
    actual_start_time TIMESTAMP,
    actual_end_time TIMESTAMP,
    leg_fare DECIMAL(6,2) DEFAULT 0.00,
    leg_status leg_status_enum DEFAULT 'scheduled',
    
    FOREIGN KEY (journey_id) REFERENCES multi_modal_journey(journey_id) ON DELETE CASCADE,
    FOREIGN KEY (asset_id) REFERENCES transport_assets(asset_id) ON DELETE SET NULL,
    FOREIGN KEY (start_location_id) REFERENCES infrastructure_nodes(node_id),
    FOREIGN KEY (end_location_id) REFERENCES infrastructure_nodes(node_id),
    CONSTRAINT valid_leg_sequence CHECK (leg_sequence > 0),
    CONSTRAINT valid_leg_timestamps CHECK (
        scheduled_start_time < scheduled_end_time AND
        (actual_start_time IS NULL OR actual_start_time <= COALESCE(actual_end_time, CURRENT_TIMESTAMP))
    ),
    CONSTRAINT non_negative_fare CHECK (leg_fare >= 0),
    UNIQUE (journey_id, leg_sequence)
);

-- 6. Dynamic Pricing Rules
CREATE TABLE dynamic_pricing_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name VARCHAR(100) NOT NULL,
    applicable_mode transport_mode_enum NOT NULL,
    condition_expression JSONB NOT NULL,
    modifier_type modifier_type_enum NOT NULL,
    modifier_value DECIMAL(5,2) NOT NULL,
    priority INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_until TIMESTAMP,
    
    CONSTRAINT valid_modifier_value CHECK (
        (modifier_type = 'multiplier' AND modifier_value > 0) OR
        (modifier_type IN ('fixed_surcharge', 'fixed_discount', 'percentage'))
    ),
    CONSTRAINT valid_date_range CHECK (
        valid_until IS NULL OR valid_from < valid_until
    )
);

-- 7. Scheduled Maintenance
CREATE TABLE scheduled_maintenance (
    schedule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_type transport_mode_enum NOT NULL,
    model VARCHAR(100),
    maintenance_type VARCHAR(100) NOT NULL,
    trigger_type trigger_type_enum NOT NULL,
    trigger_interval INTEGER NOT NULL CHECK (trigger_interval > 0),
    estimated_duration INTERVAL NOT NULL,
    parts_list JSONB,
    
    CONSTRAINT valid_parts_list CHECK (
        parts_list IS NULL OR jsonb_typeof(parts_list) = 'array'
    )
);

-- 8. Maintenance Logs
CREATE TABLE maintenance_logs (
    maintenance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_id UUID NOT NULL,
    scheduled_maintenance_id UUID,
    issue_reported TEXT NOT NULL,
    diagnosis TEXT,
    work_performed TEXT,
    maintenance_status maintenance_status_enum DEFAULT 'reported',
    reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_cost DECIMAL(8,2) DEFAULT 0.00,
    
    FOREIGN KEY (asset_id) REFERENCES transport_assets(asset_id) ON DELETE CASCADE,
    FOREIGN KEY (scheduled_maintenance_id) REFERENCES scheduled_maintenance(schedule_id) ON DELETE SET NULL,
    CONSTRAINT valid_maintenance_timeline CHECK (
        reported_at <= COALESCE(started_at, CURRENT_TIMESTAMP) AND
        (started_at IS NULL OR started_at <= COALESCE(completed_at, CURRENT_TIMESTAMP))
    ),
    CONSTRAINT non_negative_maintenance_cost CHECK (total_cost >= 0)
);

-- 9. IoT Telemetry (Partitioned table setup)
CREATE TABLE iot_telemetry (
    telemetry_id BIGSERIAL,
    asset_id UUID NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    location GEOGRAPHY(Point, 4326),
    speed_kmh DECIMAL(5,1) CHECK (speed_kmh >= 0),
    battery_level_pct DECIMAL(5,2) CHECK (battery_level_pct BETWEEN 0 AND 100),
    passenger_count INTEGER CHECK (passenger_count >= 0),
    engine_temp_c DECIMAL(5,1),
    odometer_km DECIMAL(10,2) CHECK (odometer_km >= 0),
    other_metrics JSONB,
    
    PRIMARY KEY (telemetry_id, recorded_at),
    FOREIGN KEY (asset_id) REFERENCES transport_assets(asset_id) ON DELETE CASCADE
) PARTITION BY RANGE (recorded_at);

-- Create monthly partitions for IoT Telemetry
CREATE TABLE iot_telemetry_2024_01 PARTITION OF iot_telemetry
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE iot_telemetry_2024_02 PARTITION OF iot_telemetry
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- 10. Network Incidents
CREATE TABLE network_incidents (
    incident_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_type incident_type_enum NOT NULL,
    severity severity_enum NOT NULL,
    description TEXT NOT NULL,
    affected_area GEOGRAPHY(Polygon, 4326),
    reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estimated_clearance_time TIMESTAMP,
    actual_clearance_time TIMESTAMP,
    affected_routes JSONB,
    
    CONSTRAINT valid_clearance_time CHECK (
        estimated_clearance_time IS NULL OR reported_at < estimated_clearance_time
    ),
    CONSTRAINT valid_incident_timeline CHECK (
        actual_clearance_time IS NULL OR reported_at < actual_clearance_time
    ),
    CONSTRAINT valid_affected_routes CHECK (
        affected_routes IS NULL OR jsonb_typeof(affected_routes) = 'array'
    )
);

-- 11. Subscription Plans (prerequisite for entitlements)
CREATE TABLE subscription_plans (
    plan_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_name VARCHAR(100) NOT NULL,
    description TEXT,
    base_price DECIMAL(8,2) NOT NULL CHECK (base_price >= 0),
    billing_cycle_days INTEGER NOT NULL CHECK (billing_cycle_days > 0),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 12. User Subscription Entitlements
CREATE TABLE user_subscription_entitlements (
    entitlement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    subscription_plan_id UUID NOT NULL,
    entitlement_type entitlement_type_enum NOT NULL,
    transport_mode VARCHAR(50) NOT NULL,
    limit_value INTEGER,
    limit_period limit_period_enum,
    used_count INTEGER DEFAULT 0 CHECK (used_count >= 0),
    valid_from TIMESTAMP NOT NULL,
    valid_until TIMESTAMP,
    
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (subscription_plan_id) REFERENCES subscription_plans(plan_id) ON DELETE CASCADE,
    CONSTRAINT valid_entitlement_period CHECK (
        valid_until IS NULL OR valid_from < valid_until
    ),
    CONSTRAINT valid_limit CHECK (
        (limit_value IS NULL AND limit_period IS NULL) OR
        (limit_value IS NOT NULL AND limit_period IS NOT NULL AND limit_value > 0)
    )
);


--- Analyze monthly completion rates and average durations for each transport mode over the past year, with rollup for subtotals.

select to_char(trunc(created_at, 'MM'), 'YYYY-MM') as month, transport_mode, count(*) as total_journeys, round(100 * sum(case when lag_status = 'completed' then 1 else 0 end) / count(*), 2) as completion_rate, round(avg(case when leg_status = 'completed' then extract(minute from (actual_end_time - actual_start_time)), end) as avg_duration_minutes from journey_leg jl
join multi_modal_journey mmj on jl.journey_id = mmj.journey_id
where mmj.created_at >= add_months(sysdate, -12)
group by rollup(to_char(trunc(created_at, 'MM'), 'YYYY-MM'), transport_mode) 
having count(*) > 10 order by month, completion_rate desc;

--- Calculate hourly asset utilization with moving averages and percentage changes for real-time monitoring.

with asset_utilization as (
select asset_id, to_char(recorded_at, 'YYYY-MM-DD HH24') as hour_bucket, 
count(*) as telemetry_records, avg(passenger_count) as avg_passengers, avg(speed_kmh) as avg_speed,
lag(avg(passenger_count) as avg_passengers,
avg(speed_kmh) as avg_speed,
lag(avg(passenger_count)) over (partition by asset_id order by to_char(recorded_at, 'YYYY-MM HH24')) as prev_hour_passengers from iot_telemetry where recorded_at >= sysdate-1 group by asset_id, to_char(recorded_at,'YYYY-MM-DD HH24'))

SELECT au.*,
ROUND((avg_passengers - prev_hour_passengers) / NULLIF(prev_hour_passengers, 0) * 100, 2) AS passenger_change_pct,
AVG(avg_passengers) OVER (PARTITION BY asset_id ORDER BY hour_bucket ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg_4h
FROM asset_utilization au
WHERE avg_passengers > 0;

---- Identify assets requiring predictive maintenance based on historical patterns and current telemetry data.

WITH maintenance_patterns AS (
    SELECT 
        ta.asset_type,
        ta.model,
        ml.asset_id,
        ml.maintenance_status,
        ml.total_cost,
        ml.total_downtime,
        ROUND((SYSDATE - ml.completed_at), 2) AS days_since_maintenance,
        COUNT(*) OVER (PARTITION BY ta.asset_type, ta.model) AS total_maintenances,
        AVG(ml.total_cost) OVER (PARTITION BY ta.asset_type, ta.model) AS avg_model_cost
    FROM maintenance_logs ml
    JOIN transport_assets ta ON ml.asset_id = ta.asset_id
    WHERE ml.completed_at >= ADD_MONTHS(SYSDATE, -24)
),
asset_health AS (
SELECT mp.*,
it.engine_temp_c,
it.battery_level_pct,
CASE 
            WHEN mp.days_since_maintenance > 180 THEN 'HIGH_RISK'
            WHEN mp.days_since_maintenance BETWEEN 90 AND 180 THEN 'MEDIUM_RISK'  ELSE 'LOW_RISK' END AS risk_category, PERCENT_RANK() OVER (ORDER BY mp.total_cost) AS cost_percentile FROM maintenance_patterns mp LEFT JOIN iot_telemetry it ON mp.asset_id = it.asset_id  AND it.recorded_at = (SELECT MAX(recorded_at) FROM iot_telemetry WHERE asset_id = mp.asset_id))
SELECT 
    asset_type,
    model,
    risk_category,
    COUNT(*) AS assets_at_risk,
    ROUND(AVG(total_cost), 2) AS avg_maintenance_cost,
    ROUND(AVG(days_since_maintenance), 2) AS avg_days_since_maintenance FROM asset_health GROUP BY CUBE(asset_type, model, risk_category) HAVING COUNT(*) > 0 ORDER BY asset_type, model, risk_category;

--- Analyze pricing rule effectiveness and correlation between fare changes and demand.

WITH pricing_effectiveness AS (
SELECT 
        dpr.rule_name,
        dpr.applicable_mode,
        dpr.modifier_type,
        dpr.modifier_value,
        TO_CHAR(jl.actual_start_time, 'YYYY-MM-DD') AS usage_date,
        COUNT(*) AS total_rides,
        SUM(jl.leg_fare) AS total_revenue,
        AVG(jl.leg_fare) AS avg_fare,
        LAG(AVG(jl.leg_fare)) OVER (PARTITION BY dpr.applicable_mode ORDER BY TO_CHAR(jl.actual_start_time, 'YYYY-MM-DD')) AS prev_avg_fare FROM journey_leg jl JOIN dynamic_pricing_rules dpr ON jl.transport_mode = dpr.applicable_mode
        AND jl.actual_start_time BETWEEN dpr.valid_from AND COALESCE(dpr.valid_until, SYSDATE)
        AND dpr.is_active = 1 WHERE jl.actual_start_time >= SYSDATE - 30 GROUP BY dpr.rule_name, dpr.applicable_mode, dpr.modifier_type, dpr.modifier_value,  TO_CHAR(jl.actual_start_time, 'YYYY-MM-DD'))
SELECT 
    rule_name,
    applicable_mode,
    modifier_type,
    modifier_value,
    SUM(total_rides) AS total_rides,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(avg_fare), 2) AS overall_avg_fare,
    ROUND(CORR(avg_fare, total_rides), 4) AS fare_demand_correlation,
    ROUND(100 * (AVG(avg_fare) - AVG(prev_avg_fare)) / NULLIF(AVG(prev_avg_fare), 0), 2) AS fare_change_impact FROM pricing_effectiveness GROUP BY rule_name, applicable_mode, modifier_type, modifier_value HAVING SUM(total_rides) > 100 ORDER BY total_revenue DESC;

---- Identify spatial clusters of high infrastructure usage for strategic planning.

WITH spatial_clusters AS (
    SELECT 
        in1.node_id,
        in1.node_type,
        in1.name,
        in1.location,
        COUNT(jl.leg_id) AS usage_count,
        AVG(jl.leg_fare) AS avg_fare,
        SDO_GEOM.SDO_CONVEXHULL(SDO_AGGR_UNION(SDOAGGRTYPE(in2.location, 0.005)), 0.005) AS cluster_hull
    FROM infrastructure_nodes in1
    JOIN journey_leg jl ON in1.node_id = jl.start_location_id
    JOIN infrastructure_nodes in2 ON SDO_WITHIN_DISTANCE(in2.location, in1.location, 'DISTANCE=500 UNIT=METER') = 'TRUE'
    WHERE jl.actual_start_time >= SYSDATE - 90 GROUP BY in1.node_id, in1.node_type, in1.name, in1.location HAVING COUNT(jl.leg_id) > 50),
cluster_analysis AS (
    SELECT 
        node_type,
        COUNT(*) AS hotspot_count,
        SUM(usage_count) AS total_usage,
        ROUND(AVG(usage_count), 2) AS avg_usage_per_hotspot,
        ROUND(MEDIAN(avg_fare), 2) AS median_fare FROM spatial_clusters GROUP BY node_type)
SELECT * FROM cluster_analysis ORDER BY total_usage DESC;

--- Calculate customer lifetime value and segment users for targeted marketing.

WITH customer_journeys AS (
    SELECT 
        u.user_id,
        COUNT(DISTINCT mmj.journey_id) AS total_journeys,
        SUM(jl.leg_fare) AS total_spent,
        MIN(mmj.created_at) AS first_journey_date,
        MAX(mmj.created_at) AS last_journey_date,
        COUNT(DISTINCT jl.transport_mode) AS modes_used,
        AVG((mmj.actual_arrival_time - mmj.actual_departure_time)) AS avg_journey_duration FROM users u
    JOIN multi_modal_journey mmj ON u.user_id = mmj.user_id
    JOIN journey_leg jl ON mmj.journey_id = jl.journey_id WHERE mmj.journey_status = 'completed' GROUP BY u.user_id),
customer_segments AS (
    SELECT 
        user_id,
        total_journeys,
        total_spent,
        ROUND((last_journey_date - first_journey_date), 2) AS customer_lifetime_days,
        modes_used,
        avg_journey_duration,
        NTILE(5) OVER (ORDER BY total_spent DESC) AS value_segment,
        CASE 
            WHEN total_journeys >= 10 AND modes_used >= 3 THEN 'PREMIUM'
            WHEN total_journeys BETWEEN 5 AND 9 THEN 'REGULAR' 
            ELSE 'CASUAL' END AS frequency_segment FROM customer_journeys)
SELECT 
    value_segment,
    frequency_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_lifetime_value,
    ROUND(AVG(total_journeys), 2) AS avg_journeys,
    ROUND(AVG(customer_lifetime_days), 2) AS avg_retention_days FROM customer_segments GROUP BY CUBE(value_segment, frequency_segment) ORDER BY value_segment, frequency_segment;

---- Quantify financial and operational impact of network incidents by type and severity.

WITH incident_impact AS (
    SELECT 
        ni.incident_id,
        ni.incident_type,
        ni.severity,
        ni.affected_area,
        COUNT(DISTINCT jl.journey_id) AS affected_journeys,
        SUM(jl.leg_fare) AS lost_revenue,
        AVG((jl.actual_end_time - jl.actual_start_time) - (jl.scheduled_end_time - jl.scheduled_start_time)) * 24 * 60 AS avg_delay_minutes,
        LISTAGG(DISTINCT jl.transport_mode, ', ') WITHIN GROUP (ORDER BY jl.transport_mode) AS affected_modes
    FROM network_incidents ni
    JOIN infrastructure_nodes in_nodes ON SDO_RELATE(in_nodes.location, ni.affected_area, 'MASK=ANYINTERACT') = 'TRUE'
    JOIN journey_leg jl ON in_nodes.node_id = jl.start_location_id  AND jl.actual_start_time BETWEEN ni.reported_at AND COALESCE(ni.actual_clearance_time, SYSDATE) WHERE ni.reported_at >= SYSDATE - 180
    GROUP BY ni.incident_id, ni.incident_type, ni.severity, ni.affected_area),
severity_analysis AS (
    SELECT 
        incident_type,
        severity,
        COUNT(*) AS incident_count,
        SUM(affected_journeys) AS total_affected_journeys,
        ROUND(SUM(lost_revenue), 2) AS total_lost_revenue,
        ROUND(AVG(avg_delay_minutes), 2) AS avg_delay_minutes,
        ROUND(SUM(lost_revenue) / SUM(affected_journeys), 2) AS revenue_loss_per_journey
    FROM incident_impact
    GROUP BY incident_type, severity)
SELECT * FROM severity_analysis ORDER BY total_lost_revenue DESC;

---- Analyze subscription plan performance, utilization rates, and profitability.

WITH subscription_usage AS (
    SELECT 
        usp.user_id,
        sp.plan_name,
        sp.base_price,
        usp.entitlement_type,
        usp.transport_mode,
        usp.limit_value,
        usp.used_count,
        usp.valid_from,
        usp.valid_until,
        COUNT(DISTINCT mmj.journey_id) AS journeys_taken,
        SUM(jl.leg_fare) AS actual_value,
        CASE 
            WHEN usp.limit_value IS NOT NULL THEN 
                ROUND(100 * usp.used_count / usp.limit_value, 2)
            ELSE NULL
        END AS utilization_rate
    FROM user_subscription_entitlements usp
    JOIN subscription_plans sp ON usp.subscription_plan_id = sp.plan_id
    LEFT JOIN multi_modal_journey mmj ON usp.user_id = mmj.user_id 
        AND mmj.created_at BETWEEN usp.valid_from AND COALESCE(usp.valid_until, SYSDATE)
    LEFT JOIN journey_leg jl ON mmj.journey_id = jl.journey_id
    WHERE usp.valid_until >= SYSDATE OR usp.valid_until IS NULL
    GROUP BY usp.user_id, sp.plan_name, sp.base_price, usp.entitlement_type, 
             usp.transport_mode, usp.limit_value, usp.used_count, usp.valid_from, usp.valid_until
),
plan_performance AS (
    SELECT 
        plan_name,
        entitlement_type,
        transport_mode,
        COUNT(DISTINCT user_id) AS total_subscribers,
        AVG(base_price) AS avg_plan_price,
        SUM(actual_value) AS total_actual_value,
        AVG(utilization_rate) AS avg_utilization,
        ROUND(SUM(actual_value) / SUM(base_price), 2) AS value_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY utilization_rate) AS median_utilization
    FROM subscription_usage
    GROUP BY plan_name, entitlement_type, transport_mode)

SELECT 
    pp.*,
    CASE 
        WHEN value_ratio > 2 THEN 'HIGHLY_PROFITABLE'
        WHEN value_ratio BETWEEN 1 AND 2 THEN 'PROFITABLE'
        ELSE 'UNDER_PERFORMING' END AS profitability_status FROM plan_performance pp ORDER BY total_actual_value DESC;

--- Identify peak demand patterns and correlation between demand and pricing.

WITH hourly_demand AS (
    SELECT 
        TO_CHAR(jl.actual_start_time, 'YYYY-MM-DD HH24') AS hour_bucket,
        jl.transport_mode,
        in_nodes.node_type,
        COUNT(*) AS ride_count,
        AVG(jl.leg_fare) AS avg_fare,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY jl.leg_fare) AS median_fare
    FROM journey_leg jl
    JOIN infrastructure_nodes in_nodes ON jl.start_location_id = in_nodes.node_id
    WHERE jl.actual_start_time >= SYSDATE - 90
        AND jl.actual_start_time IS NOT NULL
    GROUP BY TO_CHAR(jl.actual_start_time, 'YYYY-MM-DD HH24'), jl.transport_mode, in_nodes.node_type
),
demand_patterns AS (
    SELECT 
        EXTRACT(HOUR FROM TO_TIMESTAMP(hour_bucket, 'YYYY-MM-DD HH24')) AS hour_of_day,
        transport_mode,
        node_type,
        AVG(ride_count) AS avg_hourly_demand,
        STDDEV(ride_count) AS demand_stddev,
        AVG(avg_fare) AS avg_fare,
        CORR(ride_count, avg_fare) AS demand_fare_correlation,
        RANK() OVER (PARTITION BY transport_mode ORDER BY AVG(ride_count) DESC) AS demand_rank
    FROM hourly_demand
    GROUP BY EXTRACT(HOUR FROM TO_TIMESTAMP(hour_bucket, 'YYYY-MM-DD HH24')), transport_mode, node_type
)
SELECT 
    hour_of_day,
    transport_mode,
    node_type,
    avg_hourly_demand,
    demand_stddev,
    avg_fare,
    demand_fare_correlation,
    demand_rank,
    CASE 
        WHEN avg_hourly_demand > (AVG(avg_hourly_demand) OVER () + STDDEV(avg_hourly_demand) OVER ()) THEN 'PEAK'
        WHEN avg_hourly_demand < (AVG(avg_hourly_demand) OVER () - STDDEV(avg_hourly_demand) OVER ()) THEN 'OFF_PEAK'
        ELSE 'MODERATE'
    END AS demand_category
FROM demand_patterns
ORDER BY hour_of_day, transport_mode, avg_hourly_demand DESC;

--- Create comprehensive reliability and utilization scores for assets to guide maintenance and replacement decisions.

WITH asset_metrics AS (
    SELECT 
        ta.asset_id,
        ta.asset_type,
        ta.model,
        ta.current_status,
        COUNT(DISTINCT jl.journey_id) AS total_journeys,
        COUNT(ml.maintenance_id) AS maintenance_count,
        AVG(ml.total_downtime) AS avg_downtime_days,
        AVG(it.speed_kmh) AS avg_speed,
        AVG(it.battery_level_pct) AS avg_battery,
        COUNT(DISTINCT CASE WHEN jl.leg_status = 'completed' THEN jl.journey_id END) AS completed_journeys,
        SUM(jl.leg_fare) AS total_revenue_generated
    FROM transport_assets ta
    LEFT JOIN journey_leg jl ON ta.asset_id = jl.asset_id AND jl.actual_start_time >= SYSDATE - 180
    LEFT JOIN maintenance_logs ml ON ta.asset_id = ml.asset_id AND ml.completed_at >= SYSDATE - 365
    LEFT JOIN iot_telemetry it ON ta.asset_id = it.asset_id AND it.recorded_at >= SYSDATE - 7
    GROUP BY ta.asset_id, ta.asset_type, ta.model, ta.current_status
),
reliability_scores AS (
    SELECT 
        asset_id,
        asset_type,
        model,
        current_status,
        total_journeys,
        maintenance_count,
        avg_downtime_days,
        avg_speed,
        avg_battery,
        completed_journeys,
        total_revenue_generated,
        -- Calculate reliability score (0-100)
        ROUND(
            (COALESCE(completed_journeys, 0) * 0.3 / NULLIF(total_journeys, 0) * 100) +
            (CASE WHEN maintenance_count = 0 THEN 50 ELSE 
                50 * (1 - (maintenance_count / NULLIF(total_journeys, 0))) END) +
            (CASE WHEN avg_downtime_days IS NULL THEN 20 ELSE 
                20 * (1 - (avg_downtime_days / 30)) END)
        , 2) AS reliability_score,
        -- Calculate utilization score
        ROUND(
            (total_journeys * 0.6 / (SELECT MAX(total_journeys) FROM asset_metrics)) * 100 +
            (total_revenue_generated * 0.4 / (SELECT MAX(total_revenue_generated) FROM asset_metrics)) * 100
        , 2) AS utilization_score
    FROM asset_metrics
)
SELECT 
    asset_type,
    model,
    COUNT(*) AS asset_count,
    ROUND(AVG(reliability_score), 2) AS avg_reliability,
    ROUND(AVG(utilization_score), 2) AS avg_utilization,
    ROUND(CORR(reliability_score, utilization_score), 4) AS reliability_utilization_corr,
    SUM(CASE WHEN reliability_score < 70 THEN 1 ELSE 0 END) AS low_reliability_count,
    ROUND(100 * SUM(CASE WHEN reliability_score < 70 THEN 1 ELSE 0 END) / COUNT(*), 2) AS low_reliability_pct
FROM reliability_scores
GROUP BY asset_type, model
HAVING COUNT(*) > 5
ORDER BY avg_reliability DESC, avg_utilization DESC;

--- Identify bottlenecks in transfer stations and optimize schedule coordination between different transport modes. Helps in designing better integrated timetables.

WITH transfer_patterns AS (
    SELECT 
        jl1.transport_mode AS from_mode,
        jl2.transport_mode AS to_mode,
        jl1.end_location_id AS transfer_point,
        AVG(jl2.actual_start_time - jl1.actual_end_time) AS avg_transfer_time,
        COUNT(*) AS transfer_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (jl2.actual_start_time - jl1.actual_end_time)) AS median_transfer_time
    FROM journey_leg jl1
    JOIN journey_leg jl2 ON jl1.journey_id = jl2.journey_id 
        AND jl1.leg_sequence + 1 = jl2.leg_sequence
        AND jl1.leg_status = 'completed' 
        AND jl2.leg_status = 'completed'
    WHERE jl1.actual_end_time IS NOT NULL 
        AND jl2.actual_start_time IS NOT NULL
    GROUP BY jl1.transport_mode, jl2.transport_mode, jl1.end_location_id
)
SELECT 
    from_mode,
    to_mode,
    COUNT(DISTINCT transfer_point) AS unique_transfer_points,
    SUM(transfer_count) AS total_transfers,
    ROUND(AVG(avg_transfer_time) * 24 * 60, 2) AS avg_transfer_minutes,
    ROUND(MEDIAN(median_transfer_time) * 24 * 60, 2) AS median_transfer_minutes
FROM transfer_patterns
GROUP BY CUBE(from_mode, to_mode)
HAVING SUM(transfer_count) > 100
ORDER BY total_transfers DESC;

--- Real-time traffic management, dynamic routing, and proactive congestion mitigation strategies.

WITH congestion_analysis AS (
    SELECT 
        ta.asset_id,
        in_nodes.node_id,
        in_nodes.name AS location_name,
        TO_CHAR(it.recorded_at, 'YYYY-MM-DD HH24:MI') AS time_bucket,
        AVG(it.speed_kmh) AS avg_speed,
        COUNT(*) AS reading_count,
        LAG(AVG(it.speed_kmh)) OVER (PARTITION BY ta.asset_id ORDER BY TO_CHAR(it.recorded_at, 'YYYY-MM-DD HH24:MI')) AS prev_avg_speed,
        AVG(it.speed_kmh) OVER (PARTITION BY in_nodes.node_id ORDER BY TO_CHAR(it.recorded_at, 'YYYY-MM-DD HH24:MI') RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS hourly_avg_speed
    FROM iot_telemetry it
    JOIN transport_assets ta ON it.asset_id = ta.asset_id
    JOIN infrastructure_nodes in_nodes ON SDO_WITHIN_DISTANCE(it.location, in_nodes.location, 'DISTANCE=100 UNIT=METER') = 'TRUE'
    WHERE it.recorded_at >= SYSDATE - 1/24  -- Last hour
        AND it.speed_kmh > 0
    GROUP BY ta.asset_id, in_nodes.node_id, in_nodes.name, TO_CHAR(it.recorded_at, 'YYYY-MM-DD HH24:MI')
)
SELECT 
    location_name,
    time_bucket,
    AVG(avg_speed) AS current_speed,
    AVG(hourly_avg_speed) AS historical_baseline_speed,
    ROUND((AVG(hourly_avg_speed) - AVG(avg_speed)) / AVG(hourly_avg_speed) * 100, 2) AS speed_reduction_pct,
    CASE 
        WHEN (AVG(hourly_avg_speed) - AVG(avg_speed)) / AVG(hourly_avg_speed) > 0.5 THEN 'SEVERE_CONGESTION'
        WHEN (AVG(hourly_avg_speed) - AVG(avg_speed)) / AVG(hourly_avg_speed) > 0.3 THEN 'MODERATE_CONGESTION'
        WHEN (AVG(hourly_avg_speed) - AVG(avg_speed)) / AVG(hourly_avg_speed) > 0.1 THEN 'LIGHT_CONGESTION'
        ELSE 'NORMAL'
    END AS congestion_level
FROM congestion_analysis
GROUP BY location_name, time_bucket
HAVING COUNT(*) >= 5  -- Minimum readings for reliability
ORDER BY speed_reduction_pct DESC;

--- Customer retention strategies, targeted marketing campaigns, and proactive customer service interventions.

WITH customer_behavior AS (
    SELECT 
        u.user_id,
        COUNT(DISTINCT mmj.journey_id) AS total_journeys,
        MAX(mmj.created_at) AS last_journey_date,
        MIN(mmj.created_at) AS first_journey_date,
        COUNT(DISTINCT CASE WHEN mmj.created_at >= SYSDATE - 30 THEN mmj.journey_id END) AS journeys_last_30_days,
        COUNT(DISTINCT jl.transport_mode) AS unique_modes_used,
        AVG(mmj.total_cost) AS avg_journey_cost,
        SUM(CASE WHEN jl.leg_status = 'missed' THEN 1 ELSE 0 END) AS missed_connections,
        SUM(CASE WHEN jl.leg_status = 'completed' THEN 1 ELSE 0 END) AS completed_legs
    FROM users u
    LEFT JOIN multi_modal_journey mmj ON u.user_id = mmj.user_id
    LEFT JOIN journey_leg jl ON mmj.journey_id = jl.journey_id
    GROUP BY u.user_id
),
churn_indicators AS (
    SELECT 
        user_id,
        total_journeys,
        last_journey_date,
        journeys_last_30_days,
        unique_modes_used,
        avg_journey_cost,
        missed_connections,
        completed_legs,
        CASE 
            WHEN SYSDATE - last_journey_date > 90 THEN 1 
            ELSE 0 
        END AS is_churned,
        -- Churn probability score
        ROUND(
            (CASE WHEN journeys_last_30_days = 0 THEN 0.4 ELSE 0 END) +
            (CASE WHEN SYSDATE - last_journey_date > 60 THEN 0.3 ELSE 0 END) +
            (CASE WHEN missed_connections / NULLIF(completed_legs, 0) > 0.2 THEN 0.2 ELSE 0 END) +
            (CASE WHEN unique_modes_used = 1 THEN 0.1 ELSE 0 END)
        , 2) AS churn_probability_score
    FROM customer_behavior
)
SELECT 
    CASE 
        WHEN churn_probability_score >= 0.7 THEN 'HIGH_RISK'
        WHEN churn_probability_score >= 0.4 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_category,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_journeys), 2) AS avg_total_journeys,
    ROUND(AVG(SYSDATE - last_journey_date), 2) AS avg_days_since_last_journey,
    ROUND(AVG(missed_connections / NULLIF(completed_legs, 0)) * 100, 2) AS avg_missed_connection_rate
FROM churn_indicators
GROUP BY 
    CASE 
        WHEN churn_probability_score >= 0.7 THEN 'HIGH_RISK'
        WHEN churn_probability_score >= 0.4 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END
ORDER BY risk_category;

--- Budget planning, resource allocation, and financial forecasting considering seasonal patterns.

WITH daily_revenue AS (
    SELECT 
        TRUNC(jl.actual_start_time) AS revenue_date,
        jl.transport_mode,
        EXTRACT(YEAR FROM jl.actual_start_time) AS year,
        EXTRACT(MONTH FROM jl.actual_start_time) AS month,
        TO_CHAR(jl.actual_start_time, 'DAY') AS day_name,
        SUM(jl.leg_fare) AS daily_revenue,
        COUNT(*) AS daily_trips
    FROM journey_leg jl
    WHERE jl.actual_start_time >= ADD_MONTHS(SYSDATE, -24)
        AND jl.actual_start_time IS NOT NULL
    GROUP BY TRUNC(jl.actual_start_time), jl.transport_mode, 
             EXTRACT(YEAR FROM jl.actual_start_time), 
             EXTRACT(MONTH FROM jl.actual_start_time),
             TO_CHAR(jl.actual_start_time, 'DAY')
),
seasonal_patterns AS (
    SELECT 
        transport_mode,
        month,
        day_name,
        AVG(daily_revenue) AS avg_daily_revenue,
        STDDEV(daily_revenue) AS revenue_stddev,
        AVG(daily_trips) AS avg_daily_trips,
        CORR(daily_revenue, daily_trips) AS revenue_trips_correlation,
        LAG(AVG(daily_revenue), 12) OVER (PARTITION BY transport_mode, day_name ORDER BY month) AS prev_year_same_period
    FROM daily_revenue
    GROUP BY transport_mode, month, day_name
),
growth_trends AS (
    SELECT 
        transport_mode,
        month,
        day_name,
        avg_daily_revenue,
        prev_year_same_period,
        ROUND((avg_daily_revenue - prev_year_same_period) / NULLIF(prev_year_same_period, 0) * 100, 2) AS yoy_growth_pct,
        AVG(avg_daily_revenue) OVER (PARTITION BY transport_mode ORDER BY month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg_4month
    FROM seasonal_patterns
)
SELECT 
    transport_mode,
    month,
    day_name,
    avg_daily_revenue,
    yoy_growth_pct,
    moving_avg_4month,
    ROUND(moving_avg_4month * (1 + yoy_growth_pct/100), 2) AS forecasted_revenue
FROM growth_trends
ORDER BY transport_mode, month, day_name;

--- Strategic asset allocation, procurement planning, and maximizing revenue through optimal deployment.

WITH demand_patterns AS (
    SELECT 
        in_nodes.node_id,
        in_nodes.node_type,
        in_nodes.name,
        TO_CHAR(jl.actual_start_time, 'HH24') AS hour_of_day,
        jl.transport_mode,
        COUNT(*) AS trip_count,
        AVG(jl.leg_fare) AS avg_fare,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY jl.leg_fare) AS p80_fare
    FROM journey_leg jl
    JOIN infrastructure_nodes in_nodes ON jl.start_location_id = in_nodes.node_id
    WHERE jl.actual_start_time >= SYSDATE - 90
    GROUP BY in_nodes.node_id, in_nodes.node_type, in_nodes.name, 
             TO_CHAR(jl.actual_start_time, 'HH24'), jl.transport_mode
),
asset_availability AS (
    SELECT 
        ta.asset_type,
        TO_CHAR(it.recorded_at, 'HH24') AS hour_of_day,
        COUNT(DISTINCT ta.asset_id) AS available_assets,
        AVG(it.battery_level_pct) AS avg_battery_level
    FROM transport_assets ta
    JOIN iot_telemetry it ON ta.asset_id = it.asset_id
    WHERE it.recorded_at >= SYSDATE - 7
        AND ta.current_status = 'active'
    GROUP BY ta.asset_type, TO_CHAR(it.recorded_at, 'HH24')
),
deployment_gap AS (
    SELECT 
        dp.node_id,
        dp.node_type,
        dp.name,
        dp.hour_of_day,
        dp.transport_mode,
        dp.trip_count,
        aa.available_assets,
        dp.avg_fare,
        CASE 
            WHEN aa.available_assets = 0 THEN dp.trip_count
            ELSE GREATEST(0, dp.trip_count - aa.available_assets)
        END AS supply_gap,
        ROUND(dp.avg_fare * GREATEST(0, dp.trip_count - aa.available_assets), 2) AS potential_revenue_loss
    FROM demand_patterns dp
    LEFT JOIN asset_availability aa ON dp.transport_mode = aa.asset_type 
        AND dp.hour_of_day = aa.hour_of_day
)
SELECT 
    hour_of_day,
    transport_mode,
    node_type,
    SUM(supply_gap) AS total_supply_gap,
    SUM(potential_revenue_loss) AS total_revenue_opportunity,
    COUNT(DISTINCT node_id) AS locations_with_gap
FROM deployment_gap
WHERE supply_gap > 0
GROUP BY CUBE(hour_of_day, transport_mode, node_type)
HAVING SUM(supply_gap) > 10
ORDER BY total_revenue_opportunity DESC;

--- Maintenance budget forecasting, asset replacement planning, and cost control strategies.

WITH maintenance_trends AS (
    SELECT 
        ta.asset_type,
        ta.model,
        EXTRACT(YEAR FROM ml.completed_at) AS year,
        EXTRACT(MONTH FROM ml.completed_at) AS month,
        COUNT(*) AS maintenance_count,
        SUM(ml.total_cost) AS total_maintenance_cost,
        AVG(ml.total_downtime) AS avg_downtime_days,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ml.total_cost) AS median_cost,
        LAG(SUM(ml.total_cost), 1) OVER (PARTITION BY ta.asset_type, ta.model ORDER BY EXTRACT(YEAR FROM ml.completed_at), EXTRACT(MONTH FROM ml.completed_at)) AS prev_month_cost
    FROM maintenance_logs ml
    JOIN transport_assets ta ON ml.asset_id = ta.asset_id
    WHERE ml.completed_at >= ADD_MONTHS(SYSDATE, -36)
        AND ml.maintenance_status = 'completed'
    GROUP BY ta.asset_type, ta.model, EXTRACT(YEAR FROM ml.completed_at), EXTRACT(MONTH FROM ml.completed_at)
),
cost_analysis AS (
    SELECT 
        asset_type,
        model,
        year,
        month,
        maintenance_count,
        total_maintenance_cost,
        avg_downtime_days,
        median_cost,
        prev_month_cost,
        ROUND((total_maintenance_cost - prev_month_cost) / NULLIF(prev_month_cost, 0) * 100, 2) AS mom_cost_growth_pct,
        AVG(total_maintenance_cost) OVER (PARTITION BY asset_type, model ORDER BY year, month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS moving_avg_12m
    FROM maintenance_trends
),
asset_lifetime AS (
    SELECT 
        asset_type,
        model,
        COUNT(DISTINCT asset_id) AS total_assets,
        AVG(MONTHS_BETWEEN(SYSDATE, ta.created_at)) AS avg_age_months,
        SUM(ml.total_cost) / COUNT(DISTINCT ta.asset_id) AS avg_maintenance_cost_per_asset
    FROM transport_assets ta
    LEFT JOIN maintenance_logs ml ON ta.asset_id = ml.asset_id
    GROUP BY asset_type, model
)
SELECT 
    ca.asset_type,
    ca.model,
    ca.year,
    ca.month,
    ca.total_maintenance_cost,
    ca.mom_cost_growth_pct,
    ca.moving_avg_12m,
    al.avg_age_months,
    al.avg_maintenance_cost_per_asset,
    CASE 
        WHEN ca.mom_cost_growth_pct > 20 THEN 'ACCELERATING_COSTS'
        WHEN ca.moving_avg_12m > al.avg_maintenance_cost_per_asset * 1.5 THEN 'ABOVE_AVERAGE_COSTS'
        ELSE 'NORMAL_COSTS'
    END AS cost_trend_status
FROM cost_analysis ca
JOIN asset_lifetime al ON ca.asset_type = al.asset_type AND ca.model = al.model
ORDER BY ca.asset_type, ca.model, ca.year DESC, ca.month DESC;

--- Sustainability reporting, carbon footprint analysis, and environmental compliance monitoring.

WITH emission_factors AS (
    SELECT 'bus' AS transport_mode, 0.089 AS co2_kg_per_km FROM DUAL UNION ALL
    SELECT 'train' AS transport_mode, 0.041 AS co2_kg_per_km FROM DUAL UNION ALL
    SELECT 'metro' AS transport_mode, 0.035 AS co2_kg_per_km FROM DUAL UNION ALL
    SELECT 'scooter' AS transport_mode, 0.0 AS co2_kg_per_km FROM DUAL UNION ALL  -- Electric
    SELECT 'bicycle' AS transport_mode, 0.0 AS co2_kg_per_km FROM DUAL
),
distance_calculation AS (
    SELECT 
        jl.leg_id,
        jl.transport_mode,
        jl.actual_start_time,
        jl.actual_end_time,
        -- Calculate approximate distance using spatial functions
        SDO_GEOM.SDO_DISTANCE(
            start_loc.location, 
            end_loc.location, 
            0.005
        ) / 1000 AS distance_km,  -- Convert to kilometers
        ef.co2_kg_per_km
    FROM journey_leg jl
    JOIN infrastructure_nodes start_loc ON jl.start_location_id = start_loc.node_id
    JOIN infrastructure_nodes end_loc ON jl.end_location_id = end_loc.node_id
    JOIN emission_factors ef ON jl.transport_mode = ef.transport_mode
    WHERE jl.actual_start_time IS NOT NULL 
        AND jl.actual_end_time IS NOT NULL
        AND jl.leg_status = 'completed'
),
environmental_impact AS (
    SELECT 
        transport_mode,
        EXTRACT(YEAR FROM actual_start_time) AS year,
        EXTRACT(MONTH FROM actual_start_time) AS month,
        COUNT(*) AS trips_completed,
        SUM(distance_km) AS total_distance_km,
        SUM(distance_km * co2_kg_per_km) AS total_co2_kg,
        AVG(distance_km) AS avg_trip_distance_km
    FROM distance_calculation
    WHERE actual_start_time >= ADD_MONTHS(SYSDATE, -12)
    GROUP BY transport_mode, EXTRACT(YEAR FROM actual_start_time), EXTRACT(MONTH FROM actual_start_time)
),
modal_shift_impact AS (
    SELECT 
        year,
        month,
        SUM(total_co2_kg) AS total_emissions_kg,
        SUM(CASE WHEN transport_mode IN ('scooter', 'bicycle') THEN total_distance_km ELSE 0 END) AS green_distance_km,
        ROUND(100 * SUM(CASE WHEN transport_mode IN ('scooter', 'bicycle') THEN total_distance_km ELSE 0 END) / SUM(total_distance_km), 2) AS green_mode_share_pct,
        LAG(SUM(total_co2_kg), 1) OVER (ORDER BY year, month) AS prev_month_emissions
    FROM environmental_impact
    GROUP BY year, month
)
SELECT 
    year,
    month,
    total_emissions_kg,
    green_mode_share_pct,
    prev_month_emissions,
    ROUND((total_emissions_kg - prev_month_emissions) / NULLIF(prev_month_emissions, 0) * 100, 2) AS emission_change_pct,
    SUM(total_emissions_kg) OVER (ORDER BY year, month) AS cumulative_emissions_kg
FROM modal_shift_impact
ORDER BY year DESC, month DESC;

--- Contract compliance monitoring, performance reporting to regulators, and service quality improvement initiatives.

WITH sla_metrics AS (
    SELECT 
        jl.journey_id,
        jl.leg_id,
        jl.transport_mode,
        jl.leg_sequence,
        jl.scheduled_start_time,
        jl.actual_start_time,
        jl.scheduled_end_time,
        jl.actual_end_time,
        -- Calculate delays
        GREATEST(0, (jl.actual_start_time - jl.scheduled_start_time) * 24 * 60) AS start_delay_minutes,
        GREATEST(0, (jl.actual_end_time - jl.scheduled_end_time) * 24 * 60) AS end_delay_minutes,
        -- SLA thresholds
        CASE 
            WHEN jl.transport_mode = 'bus' THEN 5
            WHEN jl.transport_mode = 'train' THEN 3
            WHEN jl.transport_mode = 'metro' THEN 2
            ELSE 10
        END AS sla_threshold_minutes
    FROM journey_leg jl
    WHERE jl.actual_start_time IS NOT NULL 
        AND jl.scheduled_start_time IS NOT NULL
        AND jl.actual_start_time >= SYSDATE - 30
),
sla_compliance AS (
    SELECT 
        transport_mode,
        TO_CHAR(actual_start_time, 'YYYY-MM-DD') AS service_date,
        COUNT(*) AS total_services,
        SUM(CASE WHEN start_delay_minutes <= sla_threshold_minutes THEN 1 ELSE 0 END) AS on_time_starts,
        SUM(CASE WHEN end_delay_minutes <= sla_threshold_minutes THEN 1 ELSE 0 END) AS on_time_ends,
        AVG(start_delay_minutes) AS avg_start_delay,
        AVG(end_delay_minutes) AS avg_end_delay,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY start_delay_minutes) AS p95_start_delay
    FROM sla_metrics
    GROUP BY transport_mode, TO_CHAR(actual_start_time, 'YYYY-MM-DD')
),
compliance_analysis AS (
    SELECT 
        transport_mode,
        service_date,
        total_services,
        on_time_starts,
        on_time_ends,
        ROUND(100 * on_time_starts / total_services, 2) AS start_time_compliance_pct,
        ROUND(100 * on_time_ends / total_services, 2) AS end_time_compliance_pct,
        avg_start_delay,
        avg_end_delay,
        p95_start_delay,
        LAG(ROUND(100 * on_time_starts / total_services, 2), 7) OVER (PARTITION BY transport_mode ORDER BY service_date) AS prev_week_compliance
    FROM sla_compliance
    WHERE total_services >= 10  -- Minimum services for statistical significance
)
SELECT 
    transport_mode,
    service_date,
    total_services,
    start_time_compliance_pct,
    end_time_compliance_pct,
    avg_start_delay,
    p95_start_delay,
    prev_week_compliance,
    ROUND(start_time_compliance_pct - prev_week_compliance, 2) AS compliance_trend,
    CASE 
        WHEN start_time_compliance_pct >= 95 THEN 'EXCEEDS_SLA'
        WHEN start_time_compliance_pct >= 90 THEN 'MEETS_SLA'
        WHEN start_time_compliance_pct >= 85 THEN 'MARGINAL_SLA'
        ELSE 'FAILS_SLA'
    END AS sla_performance_rating
FROM compliance_analysis
ORDER BY transport_mode, service_date DESC;

--- Real-time navigation systems, dynamic routing for fleet management, and customer journey planning.

WITH current_conditions AS (
    SELECT 
        in_nodes.node_id,
        in_nodes.name,
        in_nodes.location,
        AVG(CASE WHEN it.recorded_at >= SYSDATE - 1/24 THEN it.speed_kmh ELSE NULL END) AS current_speed,
        COUNT(CASE WHEN it.recorded_at >= SYSDATE - 1/24 THEN 1 ELSE NULL END) AS recent_readings,
        AVG(it.speed_kmh) AS historical_avg_speed
    FROM infrastructure_nodes in_nodes
    LEFT JOIN iot_telemetry it ON SDO_WITHIN_DISTANCE(it.location, in_nodes.location, 'DISTANCE=200 UNIT=METER') = 'TRUE'
        AND it.recorded_at >= SYSDATE - 7
    WHERE in_nodes.node_type IN ('bus_stop', 'train_station', 'metro_station')
    GROUP BY in_nodes.node_id, in_nodes.name, in_nodes.location
),
congestion_indicators AS (
    SELECT 
        node_id,
        name,
        location,
        current_speed,
        historical_avg_speed,
        recent_readings,
        CASE 
            WHEN current_speed IS NULL THEN 'UNKNOWN'
            WHEN current_speed / NULLIF(historical_avg_speed, 0) < 0.5 THEN 'SEVERE_CONGESTION'
            WHEN current_speed / NULLIF(historical_avg_speed, 0) < 0.7 THEN 'MODERATE_CONGESTION'
            WHEN current_speed / NULLIF(historical_avg_speed, 0) < 0.9 THEN 'LIGHT_CONGESTION'
            ELSE 'CLEAR'
        END AS congestion_status,
        CASE 
            WHEN current_speed IS NULL THEN historical_avg_speed
            ELSE current_speed
        END AS effective_speed
    FROM current_conditions
),
route_segments AS (
    SELECT 
        jl.start_location_id,
        jl.end_location_id,
        AVG(SDO_GEOM.SDO_DISTANCE(start_loc.location, end_loc.location, 0.005)) AS segment_distance,
        COUNT(*) AS historical_trips,
        AVG((jl.actual_end_time - jl.actual_start_time) * 24 * 60) AS historical_travel_time_minutes
    FROM journey_leg jl
    JOIN infrastructure_nodes start_loc ON jl.start_location_id = start_loc.node_id
    JOIN infrastructure_nodes end_loc ON jl.end_location_id = end_loc.node_id
    WHERE jl.actual_start_time >= SYSDATE - 90
        AND jl.leg_status = 'completed'
    GROUP BY jl.start_location_id, jl.end_location_id
    HAVING COUNT(*) > 10
),
optimized_routes AS (
    SELECT 
        rs.start_location_id,
        start_ci.name AS start_name,
        rs.end_location_id, 
        end_ci.name AS end_name,
        rs.segment_distance,
        rs.historical_travel_time_minutes,
        start_ci.congestion_status AS start_congestion,
        end_ci.congestion_status AS end_congestion,
        COALESCE(start_ci.effective_speed, 30) AS start_speed,
        COALESCE(end_ci.effective_speed, 30) AS end_speed,
        -- Calculate adjusted travel time considering current conditions
        ROUND(
            rs.historical_travel_time_minutes * 
            (30 / NULLIF(start_ci.effective_speed, 0)) *
            (30 / NULLIF(end_ci.effective_speed, 0))
        , 2) AS adjusted_travel_time_minutes
    FROM route_segments rs
    JOIN congestion_indicators start_ci ON rs.start_location_id = start_ci.node_id
    JOIN congestion_indicators end_ci ON rs.end_location_id = end_ci.node_id
    WHERE start_ci.recent_readings >= 3 OR start_ci.recent_readings IS NULL
)
SELECT 
    start_name,
    end_name,
    segment_distance,
    historical_travel_time_minutes,
    adjusted_travel_time_minutes,
    start_congestion,
    end_congestion,
    ROUND(adjusted_travel_time_minutes - historical_travel_time_minutes, 2) AS time_difference,
    CASE 
        WHEN adjusted_travel_time_minutes > historical_travel_time_minutes * 1.3 THEN 'AVOID_ROUTE'
        WHEN adjusted_travel_time_minutes > historical_travel_time_minutes * 1.1 THEN 'CONSIDER_ALTERNATIVES'
        ELSE 'RECOMMENDED_ROUTE'
    END AS route_recommendation
FROM optimized_routes
WHERE segment_distance BETWEEN 1 AND 50  -- Reasonable route distances
ORDER BY adjusted_travel_time_minutes ASC;

--- Service quality improvement, customer experience management, and operational excellence initiatives.

-- Assuming we have a customer_feedback table
WITH operational_metrics AS (
    SELECT 
        jl.journey_id,
        AVG((jl.actual_end_time - jl.actual_start_time) - (jl.scheduled_end_time - jl.scheduled_start_time)) * 24 * 60 AS avg_delay_minutes,
        COUNT(CASE WHEN jl.leg_status = 'missed' THEN 1 END) AS missed_connections,
        COUNT(*) AS total_legs,
        AVG(jl.leg_fare) AS avg_fare,
        COUNT(DISTINCT jl.transport_mode) AS modes_used
    FROM journey_leg jl
    WHERE jl.actual_start_time IS NOT NULL
    GROUP BY jl.journey_id
),
satisfaction_data AS (
    SELECT 
        cf.journey_id,
        cf.overall_rating,
        cf.punctuality_rating,
        cf.cleanliness_rating,
        cf.value_rating
    FROM customer_feedback cf
    WHERE cf.feedback_date >= SYSDATE - 90
),
correlation_analysis AS (
    SELECT 
        cf.overall_rating,
        cf.punctuality_rating,
        cf.value_rating,
        om.avg_delay_minutes,
        om.missed_connections,
        om.avg_fare,
        om.modes_used,
        ROUND(om.missed_connections / NULLIF(om.total_legs, 0) * 100, 2) AS missed_connection_rate
    FROM satisfaction_data cf
    JOIN operational_metrics om ON cf.journey_id = om.journey_id
),
statistical_summary AS (
    SELECT 
        ROUND(CORR(overall_rating, avg_delay_minutes), 4) AS delay_satisfaction_correlation,
        ROUND(CORR(overall_rating, missed_connection_rate), 4) AS missed_connection_satisfaction_correlation,
        ROUND(CORR(overall_rating, avg_fare), 4) AS fare_satisfaction_correlation,
        ROUND(CORR(punctuality_rating, avg_delay_minutes), 4) AS delay_punctuality_correlation,
        ROUND(CORR(value_rating, avg_fare), 4) AS fare_value_correlation,
        COUNT(*) AS sample_size
    FROM correlation_analysis
),
impact_analysis AS (
    SELECT 
        CASE 
            WHEN avg_delay_minutes <= 5 THEN '0-5 min'
            WHEN avg_delay_minutes <= 10 THEN '6-10 min'
            WHEN avg_delay_minutes <= 15 THEN '11-15 min'
            ELSE '15+ min'
        END AS delay_category,
        CASE 
            WHEN missed_connection_rate = 0 THEN 'No Missed Connections'
            WHEN missed_connection_rate <= 10 THEN '1-10% Missed'
            WHEN missed_connection_rate <= 20 THEN '11-20% Missed'
            ELSE '20%+ Missed'
        END AS connection_category,
        AVG(overall_rating) AS avg_overall_rating,
        AVG(punctuality_rating) AS avg_punctuality_rating,
        COUNT(*) AS journey_count
    FROM correlation_analysis
    GROUP BY 
        CASE 
            WHEN avg_delay_minutes <= 5 THEN '0-5 min'
            WHEN avg_delay_minutes <= 10 THEN '6-10 min'
            WHEN avg_delay_minutes <= 15 THEN '11-15 min'
            ELSE '15+ min'
        END,
        CASE 
            WHEN missed_connection_rate = 0 THEN 'No Missed Connections'
            WHEN missed_connection_rate <= 10 THEN '1-10% Missed'
            WHEN missed_connection_rate <= 20 THEN '11-20% Missed'
            ELSE '20%+ Missed'
        END
)
SELECT 
    delay_category,
    connection_category,
    journey_count,
    ROUND(avg_overall_rating, 2) AS avg_rating,
    ROUND(avg_punctuality_rating, 2) AS avg_punctuality,
    ROUND((avg_overall_rating - (SELECT AVG(overall_rating) FROM satisfaction_data)) / (SELECT STDDEV(overall_rating) FROM satisfaction_data), 2) AS rating_z_score
FROM impact_analysis
UNION ALL
SELECT 
    'STATISTICAL_CORRELATIONS' AS delay_category,
    'SUMMARY' AS connection_category,
    (SELECT sample_size FROM statistical_summary) AS journey_count,
    (SELECT delay_satisfaction_correlation FROM statistical_summary) AS avg_rating,
    (SELECT fare_value_correlation FROM statistical_summary) AS avg_punctuality,
    (SELECT missed_connection_satisfaction_correlation FROM statistical_summary) AS rating_z_score
FROM DUAL
ORDER BY delay_category, connection_category;