WITH sc_addresses_periods AS (

	SELECT
		sa.schoolid,
		sa.addresstypedescriptor,
		sa.stateabbreviationdescriptor,
		sa.city,
		sa.postalcode,
		sa.streetnumbername,
		sa.apartmentroomsuitenumber,
		sa.buildingsitenumber,
		sa.congressionaldistrict,
		sa.countyfipscode,
		sa.latitude,
		sa.longitude,
		sa.nameofcounty,
		jsonb_agg(json_build_object(
				'beginDate', sa.begindate,
				'endDate', sa.enddate
			)
		) AS periods
	FROM
		{{ ref('stg_school_base') }} AS sa
	GROUP BY
		sa.schoolid,
		sa.addresstypedescriptor,
		sa.stateabbreviationdescriptor,
		sa.city,
		sa.postalcode,
		sa.streetnumbername,
		sa.apartmentroomsuitenumber,
		sa.buildingsitenumber,
		sa.congressionaldistrict,
		sa.countyfipscode,
		sa.latitude,
		sa.longitude,
		sa.nameofcounty
),

final as(
SELECT
	s_b.resourceid,
	s_b.externalid,
	s_b.resourcetype,
	s_b.operation,
	json_build_object(
		'educationOrganizationCategories', s_eoc.educationOrganizationCategories,
		'gradeLevels', s_gl.gradeLevels,
		'schoolId', s_b.schoolid,
		'charterApprovalSchoolYearTypeReference', s_b.charterApprovalSchoolYearTypeReference,
		'localEducationAgencyReference', s_b.localEducationAgencyReference,
		'addresses', s_a.addresses,
		'administrativeFundingControlDescriptor', s_b.administrativefundingcontroldescriptor,
		'charterApprovalAgencyTypeDescriptor', s_b.charterapprovalagencytypedescriptor,
		'charterStatusDescriptor', s_b.charterstatusdescriptor,
		'identificationCodes', s_ic.identificationCodes,
		'indicators	', s_i.indicators,
		'institutionTelephones', s_it.institutionTelephones,
		'internationalAddresses', s_ia.internationalAddresses,
		'internetAccessDescriptor', s_b.internetaccessdescriptor,,
		'magnetSpecialProgramEmphasisSchoolDescriptor', s_b.magnetspecialprogram__hasisschooldescriptor,
		'nameOfInstitution',s_b.nameofinstitution,
		'operationalStatusDescriptor', s_b.operationalstatusdescriptor,
		'schoolCategories', s_c.schoolCategories,
		'schoolTypeDescriptor', s_b.schooltypedescriptor,
		'shortNameOfInstitution', s_b.shortnameofinstitution,
		'titleIPartASchoolDesignationDescriptor', s_b.titleipartaschooldesignationdescriptor,
		'webSite', s_b.website,
		'periods', sap.periods

	) AS payload,
	status
FROM
	{{ ref('stg_school_base') }} AS s_b
LEFT OUTER JOIN
	{{ ref('stg_school_education_organization_categories') }} AS s_eoc
ON
	s_eoc.schoolid = s_b.schoolid
LEFT OUTER JOIN
	{{ ref('stg_school_gradelevels') }}  AS s_gl
ON
	s_gl.schoolid = s_b.schoolid
LEFT OUTER JOIN

	{{ ref('stg_school_addresses') }}  AS s_a
	LEFT JOIN
            sc_addresses_periods AS sap
        ON
            sap.schoolid = s_a.schoolid AND
            sap.addresstypedescriptor AND
		    sap.stateabbreviationdescriptor AND
            sap.city = s_a.city AND
            sap.postalcode = s_a.postalcode AND
            sap.streetnumbername = s_a.streetnumbername AND
            sap.apartmentroomsuitenumber = s_a.apartmentroomsuitenumber AND
            sap.buildingsitenumber = s_a.buildingsitenumber AND
            sap.congressionaldistrict = s_a.congressionaldistrict AND
            sap.countyfipscode = s_a.countyfipscode AND
            sap.latitude = s_a.latitude AND
            sap.longitude = s_a.longitude AND
            sap.nameofcounty = s_a.nameofcounty
        GROUP BY
            s_a.schoolid

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