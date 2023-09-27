WITH linked_condition_occurrence AS (
    SELECT * 
    FROM {{ ref('condition_occurrence') }} 
),

ehr_only_conditions AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
    FROM linked_condition_occurrence
    WHERE condition_occurrence_source = 'ehr'
),

claims_only_conditions AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
    FROM linked_condition_occurrence
    WHERE condition_occurrence_source = 'claims'
),

temporal_discrepancy AS (
    SELECT
        ehr.person_id,
        ehr.condition_concept_id,
        ehr.condition_start_date AS ehr_start_date,
        claims.condition_start_date AS claims_start_date,
        ABS(ehr.condition_start_date - claims.condition_start_date) AS day_difference
    FROM ehr_only_conditions ehr
    JOIN claims_only_conditions claims
        ON ehr.person_id = claims.person_id
        AND ehr.condition_concept_id = claims.condition_concept_id
    WHERE 
        ehr.condition_concept_id IN (
            SELECT condition_concept_id
            FROM linked_condition_occurrence
            GROUP BY person_id, condition_concept_id
            HAVING COUNT(*) = 1
        )
)

SELECT *
FROM temporal_discrepancy
