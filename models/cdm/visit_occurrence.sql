WITH ehr_visit AS (
	SELECT
		*
		, 'ehr' AS visit_occurrence_source
	FROM {{ source('cdm_ehr', 'visit_occurrence') }}
)

, claims_visit AS (
	SELECT
		*
		, 'claims' AS visit_occurrence_source
	FROM {{ source('cdm_claims', 'visit_occurrence') }}
)

, linked_person AS (
	SELECT
		linked_person_id
		, ehr_person_id
		, claims_person_id
	FROM {{ ref('linked_person') }}
)

-- Aggregate visit occurrences from EHR and Claims
, all_visits AS (
	SELECT
		ehr_visit.*
		, ehr_visit.visit_occurrence_id AS original_visit_occurrence_id
	FROM ehr_visit
		INNER JOIN linked_person
			ON ehr_visit.person_id = linked_person.ehr_person_id
	UNION ALL
	SELECT
		claims_visit.*
		, claims_visit.visit_occurrence_id AS original_visit_occurrence_id
	FROM claims_visit
		INNER JOIN linked_person
			ON claims_visit.person_id = linked_person.claims_person_id
)

-- Update person_id and visit_occurrence_id
SELECT
	linked_person_id       AS person_id
	, ROW_NUMBER() OVER () AS visit_occurrence_id
	, {{ dbt_utils.star(from=source('cdm_ehr', 'visit_occurrence'), except=['person_id', 'visit_occurrence_id']) }}
	, original_visit_occurrence_id
	, visit_occurrence_source
FROM all_visits
	INNER JOIN linked_person
		ON all_visits.person_id IN (linked_person.ehr_person_id, linked_person.claims_person_id)
