{{ config(
    materialized='table'
    )
}}

WITH final as (

SELECT
	s_b.resourceid,
	s_b.externalid,
	s_b.resourcetype,
	s_b.operation,
	s_b.status,
	CURRENT_TIME as modified_at,
	json_build_object(
		'educationOrganizationCategories', s_eoc.educationorganizationcategories,
		'gradeLevels', s_gl.gradeLevels,
		'schoolId', s_b.schoolid,
		'charterApprovalSchoolYearTypeReference', s_b.charterapprovalschoolyeartypereference,
		'localEducationAgencyReference', s_b.localeducationagencyreference,
		'addresses', s_a.addresses,
		'administrativeFundingControlDescriptor', s_b.administrativefundingcontroldescriptor,
		'charterApprovalAgencyTypeDescriptor', s_b.charterapprovalagencytypedescriptor,
		'charterStatusDescriptor', s_b.charterstatusdescriptor,
		'identificationCodes', s_ic.identificationCodes,
		'indicators	', s_i.indicators,
		'institutionTelephones', s_it.institutiontelephones,
		'internationalAddresses', s_ia.internationaladdresses,
		'internetAccessDescriptor', s_b.internetaccessdescriptor,
		'magnetSpecialProgramEmphasisSchoolDescriptor', s_b.magnetspecialprogramemphasisschooldescriptor,
		'nameOfInstitution',s_b.nameofinstitution,
		'operationalStatusDescriptor', s_b.operationalstatusdescriptor,
		'schoolCategories', s_c.schoolCategories,
		'schoolTypeDescriptor', s_b.schooltypedescriptor,
		'shortNameOfInstitution', s_b.shortnameofinstitution,
		'titleIPartASchoolDesignationDescriptor', s_b.titleipartaschooldesignationdescriptor,
		'webSite', s_b.website

	) AS payload

FROM
	{{ ref('stg_school_base') }} AS s_b
LEFT OUTER JOIN
	{{ ref('stg_school_education_organization_category') }} AS s_eoc
ON
	s_eoc.schoolid = s_b.schoolid
LEFT OUTER JOIN
	{{ ref('stg_school_gradelevels') }}  AS s_gl
ON
	s_gl.schoolid = s_b.schoolid
LEFT OUTER JOIN

	{{ ref('stg_school_addresses') }}  AS s_a
ON
	s_a.schoolid = s_b.schoolid

LEFT OUTER JOIN
	{{ ref('stg_school_identification_codes') }}  AS s_ic
ON
	s_ic.schoolid = s_b.schoolid
LEFT OUTER JOIN
	{{ ref('stg_school_indicators') }}  AS s_i
ON
	s_i.schoolid = s_b.schoolid
LEFT OUTER JOIN
	{{ ref('stg_school_institution_telephones') }}  AS s_it
ON
	s_it.schoolid = s_b.schoolid
	LEFT OUTER JOIN
	{{ ref('stg_school_international_addresses') }}  AS s_ia
ON
	s_ia.schoolid = s_b.schoolid
	LEFT OUTER JOIN
	{{ ref('stg_school_categories') }}  AS s_c
ON
	s_c.schoolid = s_b.schoolid

)

select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where modified_at > (select max(modified_at) from {{ this }})

{% endif %}


