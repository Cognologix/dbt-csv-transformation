-- Apply lookup and other business transformations at this stage
WITH sc_psir as (

    SELECT * from {{ref('cl_school_post_secondary_institution_reference')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
        sc_psir.loadid as LOADID,
		sc_psir.schoolid,
		jsonb_agg(json_build_object(
				'postSecondaryInstitutionId', sc_psir.postsecondaryinstitutionid
			
			)) AS postSecondaryInstitutionReference
   FROM
	    sc_psir

   GROUP BY
        sc_psir.loadid,
		sc_psir.schoolid

)

select * from final
