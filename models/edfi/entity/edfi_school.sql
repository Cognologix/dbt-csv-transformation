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
),
charter_waitlist AS
(
	SELECT
		scw.schoolid,
		jsonb_agg(json_build_object(
				'txBeginDate', scw.tx_begindate,
				'txEndDate', scw.tx_enddate,
				'txCharterAdmissionWaitlist', scw.tx_charteradmissionwaitlist,
				'txCharterEducationalEnrollmentCapacity', scw.tx_chartereducationalenrollmentcapacity,
				'txNumberCharterStudentsEnrolled', scw.tx_numbercharterstudentsenrolled
			) 
		) AS  charterWaitlists
	FROM
		public.school_charter_waitlist AS scw
	GROUP BY
		scw.schoolid
),
elo_activities AS
(
	SELECT
		seo.schoolid,
		jsonb_agg(json_build_object(
				'txELOActivityDescriptor', seo.tx_eloactivitydescriptor,
				'txELODaysScheduledPerYear', seo.tx_elodaysscheduledperyear,
				'txELOMinutesScheduledPerDay', seo."tx_eLOMinutesSche duledPerDay"
			)
		) AS eloActivities
	FROM
		public.school_elo_activities AS seo
	GROUP BY
		seo.schoolid
),  
elo_types AS (
	SELECT
		sset.schoolid,
		json_build_object(
			'txELOTypeDescriptor', sset.tx_elotypedescriptor,
			'txBeginDate', sset.tx_begindate,
			'txEndDate', sset.tx_enddate,
			'eloActivities', seo. eloActivities
		) AS eloTypes
	FROM
		public.school_elo_types AS sset
	LEFT JOIN
		elo_activities AS seo
	ON
		seo.schoolid = sset.schoolid
),
nslp_types AS
(
	SELECT
	snt.schoolid,
	jsonb_agg(json_build_object (
			'txNSLPTypeDescriptor', snt.tx_nslptypedescriptor,
			'txBeginDate', snt.tx_begindate,
			'txEndDate', snt.tx_enddate
		)
	) AS nslpTypes
	FROM
		public.school_nslp_types AS snt
	GROUP BY
		snt.schoolid
),
post_secondary_institution_reference AS (
	SELECT 
		spsir.schoolid,
		json_build_object(
			'postSecondaryInstitutionReference', 
				json_build_object(
					'postSecondaryInstitutionId', spsir.postsecondaryinstitutionid
				)
		) AS TPDM
	FROM
		public.school_post_secondary_institution_reference spsir
),
campus_enrollment_types AS (
	SELECT 
		scet.schoolid,
		jsonb_agg(json_build_object(
				'txCampusEnrollmentTypeDescriptor', scet.tx_campusenrollmenttypedescriptor,
				'txBeginDate', scet.tx_begindate,
				'txEndDate', scet.tx_enddate
			)
		) AS campusEnrollmentTypes
	FROM
		public.school_campus_enrollment_types AS scet
	GROUP BY
		scet.schoolid
),
extensions AS
(
	SELECT 
		sb.schoolid,
		json_build_object(
			'TexasExtensions',
			json_build_object(
				'txAdditionalDaysProgram', sb.txAdditionalDaysProgram,
				'txNumberOfBullyingIncidents', sb.txNumberOfBullyingIncidents,
				'txNumberOfCyberbullyingIncidents', sb.txNumberOfCyberbullyingIncidents,
				'txPKFullDayWaiver', sb.txPKFullDayWaiver,
				'campusEnrollmentTypes', scet.campusEnrollmentTypes,
				'charterWaitlists', scw.charterWaitlists,
				'seo.eloTypes', seo.eloTypes,
				'nslpTypes', snt.nslpTypes,
				'TPDM', sipsir.TPDM
			) 
		)
		AS _ext
	FROM 
		sc_base AS sb
	LEFT JOIN
		charter_waitlist AS scw
	ON
		scw.schoolid = sb.schoolid
	LEFT JOIN
		elo_types AS seo
	ON
		seo.schoolid = sb.schoolid
	LEFT JOIN
		nslp_types AS snt
	ON
		snt.schoolid = sb.schoolid
	LEFT JOIN
		post_secondary_institution_reference AS sipsir
	ON
		sipsir.schoolid = sb.schoolid	
	LEFT JOIN
		campus_enrollment_types AS scet
	ON 
		scet.schoolid = sb.schoolid	
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
		'webSite', sb.webSite,
		'_ext', extensions._ext
	) AS payload,
	sb.status
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
LEFT OUTER JOIN
	extensions
ON
	extensions.schoolid = sb.schoolid
