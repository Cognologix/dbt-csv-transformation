{{ config(
    materialized='incremental'
    )
}}

----------------------------------------------------------------
-- Logging invalid records from School Base entity
----------------------------------------------------------------
with err_sc_b as (
    SELECT schoolid,
        'School Base' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'nameofinstitution',nameofinstitution)) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_base')}}
    WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(nameofinstitution),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Addresses
----------------------------------------------------------------
err_sc_address as (
    SELECT schoolid,
        'School Addresses' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'addresstypedescriptor', addresstypedescriptor,
                    'stateabbreviationdescriptor', stateabbreviationdescriptor,
                    'city', city,
                    'postalcode', postalcode
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_addresses')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(addresstypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(stateabbreviationdescriptor),'') IS NULL
	    OR NULLIF(TRIM(city),'') IS NULL
	    OR NULLIF(TRIM(postalcode),'') IS NULL
	    OR NULLIF(TRIM(streetnumbername),'') IS NULL


    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Campus Enrollment Types
----------------------------------------------------------------
err_sc_cet as (
    SELECT schoolid,
        'School Campus Enrollment Types' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_campusenrollmenttypedescriptor',tx_campusenrollmenttypedescriptor
                    )) as Source_Record,
        now() as processed_at
    FROM

    {{ source('public', 'school_campus_enrollment_types')}}

    WHERE
        schoolid IS NULL
        OR NULLIF(TRIM(tx_campusenrollmenttypedescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Category
----------------------------------------------------------------
err_sc_c as (
    SELECT schoolid,
        'School Category' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'schoolcategorydescriptor',schoolcategorydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
			{{ source('public', 'school_categories')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(schoolcategorydescriptor),'') IS NULL
    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Charter Waitlist
----------------------------------------------------------------
err_sc_cw as (
    SELECT schoolid,
        'School Charter Waitlist' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_begindate', tx_begindate,
                    'tx_enddate', tx_enddate
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_charter_waitlist')}}
	WHERE
	    schoolid IS NULL
	    OR tx_begindate IS NULL
	    OR tx_enddate IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Education Organization Categories
----------------------------------------------------------------
err_sc_eoc as (
    SELECT schoolid,
        'School Education Organization Categories' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'educationorganizationcategorydescriptor',educationorganizationcategorydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_education_organization_category')}} AS sce
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(educationorganizationcategorydescriptor),'') IS NULL

    GROUP BY schoolid
),


----------------------------------------------------------------
-- Logging invalid records from School ELO Activities
----------------------------------------------------------------
err_sc_elo_a as (
    SELECT schoolid,
        'School ELO Activities' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_elo_activities')}}
	WHERE
	    schoolid IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School ELO Types
----------------------------------------------------------------
err_sc_elo_t as (
    SELECT schoolid,
        'School ELO Types' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_elo_types')}}
	WHERE
	    schoolid IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Grade Levels
----------------------------------------------------------------
err_sc_gl as (
    SELECT schoolid,
        'School Grade Levels' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'gradeleveldescriptor', gradeleveldescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_gradelevels')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(gradeleveldescriptor),'') IS NULL


    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Identification Codes
----------------------------------------------------------------
err_sc_ic as (
    SELECT schoolid,
        'School Identification Codes' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'educationorganizatio__ationsystemdescriptor',educationorganizatio__ationsystemdescriptor,
                    'identificationcode',identificationcode
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_identification_codes')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(educationorganizatio__ationsystemdescriptor),'') IS NULL
	    OR NULLIF(TRIM(identificationcode),'') IS NULL


    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Indicators
----------------------------------------------------------------
err_sc_ind as (
    SELECT schoolid,
        'School Indicators' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'indicatordescriptor',indicatordescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_indicators')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(indicatordescriptor),'') IS NULL


    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Institution Telephones
----------------------------------------------------------------
err_sc_it as (
    SELECT schoolid,
        'School Institution Telephones' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'institutiontelephonenumbertypedescriptor', institutiontelephonenumbertypedescriptor,
                    'telephonenumber', telephonenumber
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_institution_telephones')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(institutiontelephonenumbertypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(telephonenumber),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School International Addresses
----------------------------------------------------------------
err_sc_ia as (
    SELECT schoolid,
        'School International Addresses' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'addresstypedescriptor', addresstypedescriptor,
                    'countrydescriptor', countrydescriptor,
                    'addressline1', addressline1
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_international_addresses')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(CAST(addresstypedescriptor AS TEXT)),'') IS NULL
	    OR NULLIF(TRIM(CAST(countrydescriptor AS TEXT)),'') IS NULL
	    OR NULLIF(TRIM(CAST(addressline1 AS TEXT)),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School NSLP Types
----------------------------------------------------------------
err_sc_nslp_t as (
    SELECT schoolid,
        'School NSLP Types' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_nslptypedescriptor', tx_nslptypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_nslp_types')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(tx_nslptypedescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging invalid records from School Post Secondary Institution Reference
----------------------------------------------------------------
err_sc_psir as (
    SELECT schoolid,
        'School Post Secondary Institution Reference' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'postsecondaryinstitutionid', postsecondaryinstitutionid
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'school_post_secondary_institution_reference')}}
	WHERE
	    schoolid IS NULL
        OR postsecondaryinstitutionid IS NULL
    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Base entity where look-up is failed
----------------------------------------------------------------
err_lk_sc_b as (
    SELECT schoolid,
        'School Base' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'administrativefundingcontroldescriptor', administrativefundingcontroldescriptor,
                    'charterstatusdescriptor', charterstatusdescriptor,
                    'charterapprovalagencytypedescriptor', charterapprovalagencytypedescriptor,
                    'internetaccessdescriptor', internetaccessdescriptor,
                    'magnetspecialprogramemphasisschooldescriptor', magnetspecialprogram__hasisschooldescriptor,
                    'operationalstatusdescriptor', operationalstatusdescriptor,
                    'schooltypedescriptor', schooltypedescriptor,
                    'titleipartaschooldesignationdescriptor', titleipartaschooldesignationdescriptor

                    )) as Source_Record,
        now() as processed_at

    FROM
        {{ref('stg_school_base')}}
	WHERE
	    schoolid IS NULL
	    OR NULLIF(TRIM(administrativefundingcontroldescriptor),'') IS NULL
	    OR NULLIF(TRIM(charterstatusdescriptor),'') IS NULL
	    OR NULLIF(TRIM(charterapprovalagencytypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(internetaccessdescriptor),'') IS NULL
	    OR NULLIF(TRIM(magnetspecialprogram__hasisschooldescriptor),'') IS NULL
	    OR NULLIF(TRIM(operationalstatusdescriptor),'') IS NULL
	    OR NULLIF(TRIM(schooltypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(titleipartaschooldesignationdescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Addresses where lookup is failed
----------------------------------------------------------------
err_lk_sc_a_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(addresses)->>'addresstypedescriptor'
        as addresstypedescriptor,
        jsonb_array_elements(addresses)->>'stateAbbreviationDescriptor'
        as stateabbreviationdescriptor

	FROM
        {{ref('stg_school_addresses')}}
),
err_lk_sc_a as (
    SELECT
        schoolid,
        'School Addresses' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'addresstypedescriptor',addresstypedescriptor,
                    'stateabbreviationdescriptor',stateabbreviationdescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_a_tmp
	WHERE
	    NULLIF(TRIM(addresstypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(stateabbreviationdescriptor),'') IS NULL
    GROUP BY schoolid
),


----------------------------------------------------------------
-- Logging records from School Campus Enrollment Types where lookup is failed
----------------------------------------------------------------
err_lk_sc_cet_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(campusEnrollmentTypes)->>'txCampusEnrollmentTypeDescriptor'
        as tx_campusenrollmenttypedescriptor
	FROM
        {{ref('stg_school_campus_enrollment_types')}}
),

err_lk_sc_cet as (
    SELECT schoolid,
        'School Campus Enrollment Types' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_campusenrollmenttypedescriptor', tx_campusenrollmenttypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_cet_tmp
	WHERE
	    NULLIF(TRIM(tx_campusenrollmenttypedescriptor),'') IS NULL

    GROUP BY schoolid
),
----------------------------------------------------------------
-- Logging records from School Categories where lookup is failed
----------------------------------------------------------------
err_lk_sc_c_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(schoolcategories)->>'schoolcategorydescriptor'
        as schoolcategorydescriptor
	FROM
        {{ref('stg_school_categories')}}
),

err_lk_sc_c as (
    SELECT schoolid,
        'School Categories' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'schoolcategorydescriptor', schoolcategorydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_c_tmp
	WHERE
	    NULLIF(TRIM(schoolcategorydescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Education Organization Categories where lookup is failed
----------------------------------------------------------------
err_lk_sc_eoc_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(educationOrganizationCategories)->>'educationOrganizationCategoryDescriptor'
        as educationorganizationcategorydescriptor
	FROM
        {{ref('stg_school_education_organization_category')}}
),

err_lk_sc_eoc as (
    SELECT schoolid,
        'School Education Organization Categories' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'educationorganizationcategorydescriptor', educationorganizationcategorydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_eoc_tmp
	WHERE
	    NULLIF(TRIM(educationorganizationcategorydescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School ELO Activities where lookup is failed
----------------------------------------------------------------
err_lk_sc_elo_a_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(eloActivities)->>'txELOActivityDescriptor'
        as tx_eloactivitydescriptor
	FROM
        {{ref('stg_school_elo_activities')}}
),

err_lk_sc_elo_a as (
    SELECT schoolid,
        'School ELO Activities' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_eloactivitydescriptor', tx_eloactivitydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_elo_a_tmp
	WHERE
	    NULLIF(TRIM(tx_eloactivitydescriptor),'') IS NULL

    GROUP BY schoolid
),
----------------------------------------------------------------
-- Logging records from School ELO Types where lookup is failed
----------------------------------------------------------------
err_lk_sc_elo_t_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(eloTypes)->>'txELOTypeDescriptor'
        as tx_elotypedescriptor
	FROM
        {{ref('stg_school_elo_types')}}
),

err_lk_sc_elo_t as (
    SELECT schoolid,
        'School ELO Types' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_elotypedescriptor', tx_elotypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_elo_t_tmp
	WHERE
	    NULLIF(TRIM(tx_elotypedescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Identification Codes where lookup is failed
----------------------------------------------------------------
err_lk_sc_ic_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(identificationCodes)->>'educationorganizatio__ationsystemdescriptor'
        as educationorganizatio__ationsystemdescriptor
	FROM
        {{ref('stg_school_identification_codes')}}
),

err_lk_sc_ic as (
    SELECT schoolid,
        'School Identification Codes' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'educationorganizatio__ationsystemdescriptor', educationorganizatio__ationsystemdescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_ic_tmp
	WHERE
	    NULLIF(TRIM(educationorganizatio__ationsystemdescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Indicators where lookup is failed
----------------------------------------------------------------
err_lk_sc_ind_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(indicators)->>'indicatorDescriptor'
        as indicatordescriptor,
        jsonb_array_elements(indicators)->>'indicatorGroupDescriptor'
        as indicatorgroupdescriptor,
        jsonb_array_elements(indicators)->>'indicatorLevelDescriptor'
        as indicatorleveldescriptor

	FROM
        {{ref('stg_school_indicators')}}
),

err_lk_sc_ind as (
    SELECT
         schoolid,
        'School Indicators' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'indicatordescriptor', indicatordescriptor,
                    'indicatorgroupdescriptor', indicatorgroupdescriptor,
                    'indicatorleveldescriptor', indicatorleveldescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_ind_tmp
	WHERE
	    NULLIF(TRIM(indicatordescriptor),'') IS NULL
	    OR NULLIF(TRIM(indicatorgroupdescriptor),'') IS NULL
	    OR NULLIF(TRIM(indicatorleveldescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School Institution Telephones where lookup is failed
----------------------------------------------------------------
err_lk_sc_it_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(institutionTelephones)->>'institutionTelephoneNumberTypeDescriptor'
        as institutiontelephonenumbertypedescriptor

	FROM
        {{ref('stg_school_institution_telephones')}}
),

err_lk_sc_it as (
    SELECT schoolid,
        'School Institution Telephones' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'institutiontelephonenumbertypedescriptor', institutiontelephonenumbertypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_it_tmp
	WHERE
	    NULLIF(TRIM(institutiontelephonenumbertypedescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School International Addresses where lookup is failed
----------------------------------------------------------------
err_lk_sc_ia_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(internationalAddresses)->>'addressTypeDescriptor'
        as addresstypedescriptor,
        jsonb_array_elements(internationalAddresses)->>'countryDescriptor'
        as countrydescriptor
	FROM
        {{ref('stg_school_international_addresses')}}
),

err_lk_sc_ia as (
    SELECT schoolid,
        'School International Addresses' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'addresstypedescriptor', addresstypedescriptor,
                    'countrydescriptor', countrydescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_ia_tmp
	WHERE
	    NULLIF(TRIM(addresstypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(countrydescriptor),'') IS NULL


    GROUP BY schoolid
),

----------------------------------------------------------------
-- Logging records from School NSLP Types  where lookup is failed
----------------------------------------------------------------
err_lk_sc_tx_nslp_y_tmp as (
    SELECT
        schoolid,
        jsonb_array_elements(nslpTypes)->>'txNSLPTypeDescriptor'
        as tx_nslptypedescriptor
	FROM
        {{ref('stg_school_nslp_types')}}
),

err_lk_sc_tx_nslp_y as (
    SELECT schoolid,
        'School NSLP Types' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'schoolid', schoolid,
                    'tx_nslptypedescriptor', tx_nslptypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_sc_tx_nslp_y_tmp
	WHERE
	    NULLIF(TRIM(tx_nslptypedescriptor),'') IS NULL

    GROUP BY schoolid
),

----------------------------------------------------------------
-- Collecting all failures in error table
----------------------------------------------------------------
final as (
    SELECT * from err_sc_b
    UNION
    SELECT * from err_sc_address
    UNION
    SELECT * from err_sc_cet
    UNION
    SELECT * from err_sc_c
    UNION
    SELECT * from err_sc_eoc
    UNION
    SELECT * from err_sc_elo_a
    UNION
    SELECT * from err_sc_elo_t
    UNION
    SELECT * from err_sc_gl
    UNION
    SELECT * from err_sc_ic
    UNION
    SELECT * from err_sc_ind
    UNION
    SELECT * from err_sc_it
    UNION
    SELECT * from err_sc_ia
    UNION
    SELECT * from err_sc_nslp_t
    UNION
    SELECT * from err_sc_psir
    UNION
    SELECT * from err_lk_sc_b
    UNION
    SELECT * from err_lk_sc_a
    UNION
    SELECT * from err_lk_sc_cet
    UNION
    SELECT * from err_lk_sc_c
    UNION
    SELECT * from err_lk_sc_eoc
    UNION
    SELECT * from err_lk_sc_elo_a
    UNION
    SELECT * from err_lk_sc_elo_t
    UNION
    SELECT * from err_lk_sc_ic
    UNION
    SELECT * from err_lk_sc_ind
    UNION
    SELECT * from err_lk_sc_it
    UNION
    SELECT * from err_lk_sc_ia
    UNION
    SELECT * from err_lk_sc_tx_nslp_y

)

----------------------------------------------------------------
-- Incremental flag set for maintaining previous load's error data
----------------------------------------------------------------
select {{ var('LOADID',-1) }} as LOADID, * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})

{% endif %}