{% test is_vendor_id_valid(model, column_name) %}

WITH all_values AS (
    SELECT
        VendorID,
        count(*) as counts
    FROM {{ model }}
    GROUP BY VendorID
)
SELECT *
FROM all_values
WHERE VendorID NOT IN (1,2)

{% endtest %}