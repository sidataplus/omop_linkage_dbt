WITH claims AS (
	SELECT *
	FROM {{ source('cdm_claims', 'person') }}
)

, linked_person AS (
	SELECT
		linked_person_id
		, claims_person_id
	FROM {{ ref('linked_person') }}
)

-- use claims as base for person table, change person_id to linked_person_id
SELECT
	linked_person_id AS person_id
	, {{ dbt_utils.star(from=source('cdm_claims', 'person'), except=['person_id']) }}
FROM claims
	INNER JOIN linked_person
		ON claims.person_id = linked_person.claims_person_id
