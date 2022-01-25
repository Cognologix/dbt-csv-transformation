-- Apply lookup and other business transformations at this stage
WITH s_b as (

    SELECT
    	distinct schoolid,
    	        operation,
		        administrativefundingcontroldescriptor,
		        charterapprovalagencytypedescriptor,
		        charterapprovalschoolyeartypereference_schoolyear,
                localeducationagencyid,
                charterstatusdescriptor,
                internetaccessdescriptor,
                magnetspecialprogram__hasisschooldescriptor,
                nameofinstitution,
                operationalstatusdescriptor,
                schooltypedescriptor,
                shortnameofinstitution,
                titleipartaschooldesignationdescriptor,
                website,
                tx_pkfulldaywaiver,
                tx_additionaldaysprogram,
                tx_numberofbullyingincidents,
                tx_umberofcyberbullyingincidentsc

    from
        {{ref('raw_school_base')}}
 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		s_b.schoolid,
		s_b.operation,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'schoolid', s_b.schoolid
		) AS externalid,
		'SCHOOL' AS resourcetype,
		0 AS status,
		jsonb_agg(json_build_object(
				'schoolYear', s_b.charterapprovalschoolyeartypereference_schoolyear

			)AS charterApprovalSchoolYearTypeReference,
			    json_build_object(
				'localEducationAgencyId', s_b.localeducationagencyid

			) AS localEducationAgencyReference,
            )
            s_b.administrativefundingcontroldescriptor,
            s_b.charterstatusdescriptor,
            s_b.charterapprovalagencytypedescriptor,
            s_b.internetaccessdescriptor,
            s_b.magnetspecialprogram__hasisschooldescriptor,
            s_b.nameofinstitution,
            s_b.operationalstatusdescriptor,
            s_b.schooltypedescriptor,
            s_b.shortnameofinstitution,
            s_b.titleipartaschooldesignationdescriptor,
            s_b.website,
            s_b.tx_pkfulldaywaiver,
            s_b.tx_additionaldaysprogram,
            s_b.tx_numberofbullyingincidents,
            s_b.tx_umberofcyberbullyingincidentsc
   FROM
	    s_b
   GROUP BY
		s_b.schoolid
)

select * from final