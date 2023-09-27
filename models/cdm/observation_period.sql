WITH ehr_observation_period AS (
	SELECT
		*
		, 'ehr' AS observation_period_source
	FROM {{ source('cdm_ehr', 'observation_period') }}
)

, claims_observation_period AS (
	SELECT
		*
		, 'claims' AS observation_period_source
	FROM {{ source('cdm_claims', 'observation_period') }}
)

, linked_person AS (
	SELECT
		linked_person_id
		, ehr_person_id
		, claims_person_id
	FROM {{ ref('linked_person') }}
)

-- Aggregate observation periods from EHR and Claims
, all_observation_periods AS (
	SELECT
		ehr_observation_period.*
		, ehr_observation_period.observation_period_id AS original_observation_period_id
	FROM ehr_observation_period
		INNER JOIN linked_person
			ON ehr_observation_period.person_id = linked_person.ehr_person_id
	UNION ALL
	SELECT
		claims_observation_period.*
		, claims_observation_period.observation_period_id AS original_observation_period_id
	FROM claims_observation_period
		INNER JOIN linked_person
			ON claims_observation_period.person_id = linked_person.claims_person_id
)

-- Update person_id and observation_period_id
SELECT
	linked_person_id       AS person_id
	, ROW_NUMBER() OVER () AS observation_period_id
	, {{ dbt_utils.star(from=source('cdm_ehr', 'observation_period'), except=['person_id', 'observation_period_id']) }}
	, original_observation_period_id
	, observation_period_source
FROM all_observation_periods
	INNER JOIN linked_person
		ON all_observation_periods.person_id IN (linked_person.ehr_person_id, linked_person.claims_person_id)
