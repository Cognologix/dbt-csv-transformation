-- Apply lookup and other business transformations at this stage
WITH sc_it as (

    SELECT
    	distinct schoolid,
		        institutiontelephonenumbertypedescriptor,
		        telephonenumber
    from
        {{ref('raw_school_institution_telephones')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_it.schoolid,
		jsonb_agg(json_build_object(
				'institutionTelephoneNumberTypeDescriptor', sc_it.institutiontelephonenumbertypedescriptor,
				'telephoneNumber', sc_it.telephonenumber

			)) AS institutionTelephones
   FROM
	    sc_it
   GROUP BY
		sc_it.schoolid

)

select * from final

