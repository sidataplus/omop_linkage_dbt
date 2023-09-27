WITH ehr_condition AS (
	SELECT
		*
		, 'ehr' AS condition_occurrence_source
	FROM {{ source('cdm_ehr', 'condition_occurrence') }}
)

, claims_condition AS (
	SELECT
		*
		, 'claims' AS condition_occurrence_source
	FROM {{ source('cdm_claims', 'condition_occurrence') }}
)

, linked_person AS (
	SELECT
		linked_person_id
		, ehr_person_id
		, claims_person_id
	FROM {{ ref('linked_person') }}
)

-- New visit_occurrence table for reference
, new_visit_occurrence AS (
	SELECT
		visit_occurrence_id
		, original_visit_occurrence_id
		, visit_occurrence_source
	FROM {{ ref('visit_occurrence') }}  -- Replace with your new visit_occurrence table name
)

-- Aggregate condition occurrences from EHR and Claims
, all_conditions AS (
	SELECT
		ehr_condition.*
		, ehr_condition.condition_occurrence_id AS original_condition_occurrence_id
	FROM ehr_condition
		INNER JOIN linked_person
			ON ehr_condition.person_id = linked_person.ehr_person_id
	UNION ALL
	SELECT
		claims_condition.*
		, claims_condition.condition_occurrence_id AS original_condition_occurrence_id
	FROM claims_condition
		INNER JOIN linked_person
			ON claims_condition.person_id = linked_person.claims_person_id
)

-- Update person_id, condition_occurrence_id, and visit_occurrence_id
SELECT
	linked_person_id       AS person_id
	, ROW_NUMBER() OVER () AS condition_occurrence_id
	, new_visit_occurrence.visit_occurrence_id  -- Updated visit_occurrence_id
	, , {{ dbt_utils.star(from=source('cdm_ehr', 'condition_occurrence'), except=['person_id', 'condition_occurrence_id', 'visit_occurrence_id']) }}
{{ dbt_utils.star(from=source('cdm_ehr', 'condition_occurrence'), except=['person_id', 'condition_occurrence_id', 'visit_occurrence_id']) }}
{{ dbt_utils.star(from=source('cdm_ehr', 'condition_occurrence'), except=['person_id', 'condition_occurrence_id', 'visit_occurrence_id']) }}
	, original_condition_occurrence_id
	, condition_occurrence_source
FROM all_conditions
	INNER JOIN linked_person
		ON all_conditions.person_id IN (linked_person.ehr_person_id, linked_person.claims_person_id)
	INNER JOIN new_visit_occurrence
		ON all_conditions.visit_occurrence_id = new_visit_occurrence.original_visit_occurrence_id
			AND all_conditions.condition_occurrence_source = new_visit_occurrence.visit_occurrence_source
