-- Apply lookup and other business transformations at this stage
WITH sc_ic as (

    SELECT
    	distinct schoolid,
		        educationorganizatio__ationsystemdescriptor,
		        identificationcode

    from
        {{ref('raw_school_identification_codes')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_ic.schoolid,
		jsonb_agg(json_build_object(
				'educationOrganizationIdentificationSystemDescriptor', sc_ic.educationorganizatio__ationsystemdescriptor,
				'identificationCode', sc_ic.identificationcode

			)) AS identificationCodes
   FROM
	    sc_ic
   GROUP BY
		sc_ic.schoolid

)

select * from final

