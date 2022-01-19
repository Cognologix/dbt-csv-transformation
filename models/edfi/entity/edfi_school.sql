WITH sc_base AS (
    SELECT
		sb.schoolid,
		uuid_generate_v4() AS resourceid, 
		json_build_object(
			'schoolid', sb.schoolid
		)AS externalid,
		'SCHOOL' AS resourcetype,
		sb.operation,
		0 AS status,
		json_build_object(
			'localEducationAgencyId', sb.localeducationagencyid
		) AS localEducationAgencyReference,
		sb.administrativeFundingControlDescriptor,
		sb.charterstatusdescriptor,
		sb.internetaccessdescriptor,
		sb.magnetspecialprogram__hasisschooldescriptor AS magnetSpecialProgramEmphasisSchoolDescriptor,
		sb.nameofinstitution AS nameOfInstitution,
		sb.operationalstatusdescriptor AS operationalStatusDescriptor,
		sb.schooltypedescriptor AS schoolTypeDescriptor,
		sb.shortnameofinstitution AS shortNameOfInstitution,
		sb.titleipartaschooldesignationdescriptor AS titleIPartASchoolDesignationDescriptor,
		sb.website AS webSite,
		sb.tx_additionaldaysprogram AS txAdditionalDaysProgram,
		sb.tx_numberofbullyingincidents AS txNumberOfBullyingIncidents,
		sb.tx_numberofcyberbullyingincidentsc AS txNumberOfCyberbullyingIncidents,
		sb.tx_pkfulldaywaiver AS txPKFullDayWaiver
	FROM
        public.school_base AS sb
),
sc_education_organization_category AS (
	SELECT
		seoc.schoolid,
		jsonb_agg(json_build_object(
				'educationorganizationcategorydescriptor', seoc.educationorganizationcategorydescriptor
            )
        ) AS educationOrganizationCategories
	FROM
		public.school_education_organization_category AS seoc
	GROUP BY
		seoc.schoolid
),
sc_gradeLevels AS (
	SELECT
		sg.schoolid,
		jsonb_agg(json_build_object(
				'gradeLevelDescriptor', sg.gradeleveldescriptor
            )
        ) AS gradeLevels
	FROM
		public.school_gradeLevels AS sg
	GROUP BY
		sg.schoolid
),
sc_addresses_periods AS (
	SELECT
		sa.schoolid,
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
		public.school_addresses AS sa
	GROUP BY
		sa.schoolid,
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
sc_addresses AS (
	SELECT
		sa.schoolid,
		jsonb_agg(json_build_object(
				'addressTypeDescriptor', sa.addresstypedescriptor,
				'stateAbbreviationDescriptor', sa.stateabbreviationdescriptor,
				'city', sa.city,
				'postalCode', sa.postalcode,
				'streetNumberName', sa.streetnumbername,
				'localeDescriptor', sa.localedescriptor,
				'apartmentRoomSuiteNumber', sa.apartmentroomsuitenumber,
				'buildingSiteNumber', sa.buildingsitenumber,
				'congressionalDistrict', sa.congressionaldistrict,
				'countyFIPSCode', sa.countyfipscode,
				'doNotPublishIndicator', sa.donotpublishindicator,
				'latitude', sa.latitude,
				'longitude', sa.longitude,
				'nameOfCounty', sa.nameofcounty,
				'periods', sap.periods
            )
        ) AS addresses
	FROM
		public.school_addresses AS sa
	LEFT JOIN 
		sc_addresses_periods AS sap
	ON
		sap.schoolid = sa.schoolid AND
		sap.city = sa.city AND
		sap.postalcode = sa.postalcode AND
		sap.streetnumbername = sa.streetnumbername AND
		sap.apartmentroomsuitenumber = sa.apartmentroomsuitenumber AND
		sap.buildingsitenumber = sa.buildingsitenumber AND
		sap.congressionaldistrict = sa.congressionaldistrict AND
		sap.countyfipscode = sa.countyfipscode AND
		sap.latitude = sa.latitude AND
		sap.longitude = sa.longitude AND
		sap.nameofcounty = sa.nameofcounty
	GROUP BY
		sa.schoolid
),
sc_identification_codes AS (
	SELECT
		sic.schoolid,
		jsonb_agg(json_build_object(
				'educationOrganizationIdentificationSystemDescriptor', sic.educationorganizatio__ationsystemdescriptor,
				'identificationCode', sic.identificationcode            
			)
        ) AS identificationCodes
	FROM
		public.school_identification_codes AS sic
	GROUP BY 
		sic.schoolid
),
sc_indicators_periods AS (
	SELECT
		si.schoolid,
		si.indicatorvalue,
		jsonb_agg(json_build_object(	
				'beginDate', si.begindate,
				'endDate', si.enddate
			)	
		) AS periods
	FROM
		public.school_indicators AS si
	GROUP BY
		si.schoolid,
		si.indicatorvalue
),
sc_indicators AS (
	SELECT
		si.schoolid,
		jsonb_agg(json_build_object(
				'indicatorDescriptor', si.indicatordescriptor,
				'indicatorGroupDescriptor', si.indicatorgroupdescriptor,
				'indicatorLevelDescriptor', si.indicatorleveldescriptor,
				'designatedBy', si.designatedby,
				'indicatorValue', si.indicatorvalue,
				'periods', sip.periods
			)
		) AS indicators
	FROM
		public.school_indicators AS si
	LEFT JOIN
		sc_indicators_periods	AS sip
	ON
		sip.schoolid = si.schoolid AND
		sip.indicatorvalue = si.indicatorvalue
	GROUP BY
		si.schoolid
),
sc_institution_telephones AS (
	SELECT
		sit.schoolid,
		jsonb_agg(json_build_object(
				'institutionTelephoneNumberTypeDescriptor', sit.institutiontelephonenumbertypedescriptor,
				'telephoneNumber', sit.telephonenumber
			)
		) AS institutionTelephones
	FROM
		public.school_institution_telephones AS sit
	GROUP BY sit.schoolid
),
sc_international_addresses AS (
	SELECT
		sia.schoolid,
		jsonb_agg(json_build_object(
				'addressTypeDescriptor', sia.addresstypedescriptor,
				'countryDescriptor', sia.countrydescriptor,
				'addressLine1', sia.addressline1,
				'addressLine2', sia.addressline2,
				'addressLine3', sia.addressline3,
				'addressLine4', sia.addressline4,
				'beginDate', sia.begindate,
				'endDate', sia.enddate,
				'latitude', sia.latitude,
				'longitude', sia.longitude
			)
		) AS internationalAddresses
	FROM
		public.school_international_addresses AS sia
	GROUP BY
		sia.schoolid
),
sc_categories AS (
	SELECT
		sc.schoolid,
		jsonb_agg(json_build_object(
				'schoolCategoryDescriptor', sc.schoolcategorydescriptor
			)
		) AS schoolCategories
	FROM
		public.school_categories AS sc
	GROUP BY
		sc.schoolid
)
SELECT 
	resourceid,
	externalid,
	resourcetype,
	operation,
	json_build_object(
		'educationOrganizationCategories', seoc.educationOrganizationCategories,
		'gradeLevels', sg.gradeLevels,
		'schoolId', sb.schoolid,
		'localEducationAgencyReference', sb.localEducationAgencyReference,
		'addresses', sa.addresses,
		'administrativeFundingControlDescriptor', sb.administrativefundingcontroldescriptor,
		'charterStatusDescriptor', sb.charterStatusDescriptor,
		'identificationCodes', sic.identificationCodes,
		'indicators', si.indicators,
		'institutionTelephones', sit.institutionTelephones,
		'internetAccessDescriptor', sb.internetaccessdescriptor,
		'magnetSpecialProgramEmphasisSchoolDescriptor', sb.magnetSpecialProgramEmphasisSchoolDescriptor,
		'nameOfInstitution', sb.nameOfInstitution,
		'operationalStatusDescriptor', sb.operationalStatusDescriptor,
		'internationalAddresses', sia.internationalAddresses,
		'schoolCategories', sc.schoolCategories,
		'schoolTypeDescriptor', sb.schoolTypeDescriptor,
		'shortNameOfInstitution', sb.shortNameOfInstitution,
		'titleIPartASchoolDesignationDescriptor', sb.titleIPartASchoolDesignationDescriptor,
		'webSite', sb.webSite
	) AS payload,
	sb.status
FROM 
	sc_base AS sb
LEFT OUTER JOIN
	sc_education_organization_category AS seoc
ON 
	seoc.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_gradeLevels AS sg
ON 
	sg.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_addresses AS sa
ON 
	sa.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_identification_codes AS sic
ON 
	sic.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_indicators AS si
ON 
	si.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_institution_telephones AS sit
ON
	sit.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_international_addresses AS sia
ON
	sia.schoolid = sb.schoolid
LEFT OUTER JOIN
	sc_categories AS sc
ON
	sc.schoolid = sb.schoolid
