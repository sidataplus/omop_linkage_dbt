WITH claims AS (
    SELECT
        person_id,
        gender_concept_id,
        year_of_birth,
        person_source_value
    FROM {{ source('cdm_claims', 'person') }}
),

ehr AS (
    SELECT
        person_id,
        gender_concept_id,
        year_of_birth,
        person_source_value
    FROM {{ source('cdm_ehr', 'person') }}
),

linked_person AS (
    SELECT
        ROW_NUMBER() OVER () AS linked_person_id,
        claims.person_id AS claims_person_id,
        ehr.person_id AS ehr_person_id
    FROM
        claims
    INNER JOIN ehr
        ON claims.person_source_value = ehr.person_source_value
)

SELECT
    CASE 
        WHEN claims.gender_concept_id != ehr.gender_concept_id OR claims.year_of_birth != ehr.year_of_birth THEN TRUE
        ELSE FALSE
    END AS has_discrepancy,
    linked_person.linked_person_id,
    claims.person_id AS claims_person_id,
    ehr.person_id AS ehr_person_id,
    claims.gender_concept_id AS claims_gender_concept_id,
    ehr.gender_concept_id AS ehr_gender_concept_id,
    claims.year_of_birth AS claims_year_of_birth,
    ehr.year_of_birth AS ehr_year_of_birth
FROM linked_person
INNER JOIN claims
    ON linked_person.claims_person_id = claims.person_id
INNER JOIN ehr
    ON linked_person.ehr_person_id = ehr.person_id
ORDER BY has_discrepancy DESC
