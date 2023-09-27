WITH claims AS (
	SELECT
		person_id
		, person_source_value
	FROM {{ source('cdm_claims', 'person') }}
)

, ehr AS (
	SELECT
		person_id
		, person_source_value
	FROM {{ source('cdm_ehr', 'person') }}
)

SELECT
	ROW_NUMBER() OVER () AS linked_person_id
	, claims.person_id   AS claims_person_id
	, ehr.person_id      AS ehr_person_id
FROM
	claims
	INNER JOIN ehr
		ON claims.person_source_value = ehr.person_source_value
