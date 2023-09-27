WITH claims AS (
    SELECT
        person_id,
        person_source_value
    FROM {{ source('cdm_claims', 'person') }}
),

ehr AS (
    SELECT
        person_id,
        person_source_value
    FROM {{ source('cdm_ehr', 'person') }}
),

matched_persons AS (
    SELECT
        claims.person_id AS claims_person_id,
        ehr.person_id AS ehr_person_id
    FROM
        claims
    INNER JOIN ehr
        ON claims.person_source_value = ehr.person_source_value
),

unmatched_persons AS (
    SELECT
        claims.person_id AS claims_person_id,
        ehr.person_id AS ehr_person_id
    FROM
        claims
    FULL OUTER JOIN ehr
        ON claims.person_source_value = ehr.person_source_value
    WHERE 
        claims.person_id IS NULL OR
        ehr.person_id IS NULL
)

SELECT 
    'Claims' AS source,
    COUNT(matched_persons.claims_person_id) AS matched,
    COUNT(unmatched_persons.claims_person_id) AS unmatched,
    COUNT(claims.person_id) AS total,
    ROUND(100.0 * COUNT(matched_persons.claims_person_id) / COUNT(claims.person_id), 2) AS percentage_mapped
FROM claims
LEFT JOIN matched_persons
    ON claims.person_id = matched_persons.claims_person_id
LEFT JOIN unmatched_persons
    ON claims.person_id = unmatched_persons.claims_person_id
UNION ALL
SELECT 
    'EHR' AS source,
    COUNT(matched_persons.ehr_person_id) AS matched,
    COUNT(unmatched_persons.ehr_person_id) AS unmatched,
    COUNT(ehr.person_id) AS total,
    ROUND(100.0 * COUNT(matched_persons.ehr_person_id) / COUNT(ehr.person_id), 2) AS percentage_mapped
FROM ehr
LEFT JOIN matched_persons
    ON ehr.person_id = matched_persons.ehr_person_id
LEFT JOIN unmatched_persons
    ON ehr.person_id = unmatched_persons.ehr_person_id
