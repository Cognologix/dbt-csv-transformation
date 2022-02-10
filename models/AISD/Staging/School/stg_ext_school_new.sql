WITH sc_ext_new as (

    SELECT
        sc_b.loadid,
        sc_b.schoolid,
        json_build_object(
				'txAdditionalDaysProgram', sc_b.tx_additionaldaysprogram,
				'txNumberOfBullyingIncidents', sc_b.tx_numberofbullyingincidents,
				'txNumberOfCyberbullyingIncidents', sc_b.tx_numberofcyberbullyingincidents,
				'txPKFullDayWaiver', sc_b.tx_pkfulldaywaiver,
				'campusEnrollmentTypes',sc_cet.campusEnrollmentTypes,
				'charterWaitlists',sc_cw.charterWaitlist,
				'eloTypes', sc_et.eloTypes,
				'nslpTypes', sc_nslp.nslpTypes
		) AS TexasExtensions,
		json_build_object(
				'postSecondaryInstitutionReference', sc_psir.postSecondaryInstitutionReference
		) AS TPDM

    FROM
        {{ref('stg_school_base')}} as sc_b
    LEFT JOIN
        {{ref('stg_school_campus_enrollment_types')}} AS sc_cet
    ON
        sc_cet.schoolid = sc_b.schoolid
    LEFT JOIN
        {{ref('stg_school_charter_waitlist')}} AS sc_cw
    ON
        sc_cw.schoolid = sc_b.schoolid
    LEFT JOIN
        {{ref('stg_school_elo_types')}} AS sc_et
    ON
        sc_et.schoolid = sc_b.schoolid
    LEFT JOIN
        {{ref('stg_school_nslp_types')}} AS sc_nslp
    ON
        sc_nslp.schoolid = sc_b.schoolid
    LEFT JOIN
        {{ref('stg_school_post_secondary_institution_reference')}} AS sc_psir
    ON
        sc_psir.schoolid = sc_b.schoolid
)
select * from sc_ext_new