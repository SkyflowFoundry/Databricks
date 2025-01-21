USE hive_metastore.default;

WITH grouped_data AS (
    SELECT
        1 AS group_id,
        COLLECT_LIST(name) AS names,
        COLLECT_LIST(employee_id) AS employee_ids,
        COLLECT_LIST(role) AS roles,
        COLLECT_LIST(department) AS departments,
        COLLECT_LIST(date_joined) AS date_joineds,
        COLLECT_LIST(age) AS ages,
        COLLECT_LIST(business_unit) AS business_units,
        COLLECT_LIST(security_clearance) AS security_clearances
    FROM employee_data
    GROUP BY group_id
),
detokenized_batches AS (
    SELECT
        skyflow_bulk_detokenize(names, current_user()) AS detokenized_names,
        employee_ids,
        roles,
        departments,
        date_joineds,
        ages,
        business_units,
        security_clearances
    FROM grouped_data
),
exploded_data AS (
    SELECT
        employee_ids[pos] AS employee_id,
        detokenized_names[pos] AS detokenized_name,
        roles[pos] AS role,
        departments[pos] AS department,
        date_joineds[pos] AS date_joined,
        ages[pos] AS age,
        business_units[pos] AS business_unit,
        security_clearances[pos] AS security_clearance
    FROM detokenized_batches
    LATERAL VIEW POSEXPLODE(detokenized_names) AS pos, detokenized_name
)
SELECT
    detokenized_name as name,
    role,
    department,
    date_joined,
    age,
    business_unit,
    security_clearance
FROM exploded_data;