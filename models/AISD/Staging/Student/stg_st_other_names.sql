-- Apply lookup and other business transformations at this stage
WITH son as (

    SELECT
    	distinct studentuniqueid,
		        othernametypedescriptor,
		        firstname,
		        generationcodesuffix,
		        lastsurname,
		        middlename,
		        personaltitleprefix
    from
        {{ref('raw_st_other_names')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		son.studentuniqueid,
		jsonb_agg(json_build_object(
				'otherNameTypeDescriptor', son.othernametypedescriptor,
				'firstName', son.firstname,
				'generationCodeSuffix', son.generationcodesuffix,
				'lastSurname', son.lastsurname,
				'middleName', son.middlename,
				'personalTitlePrefix', son.personaltitleprefix
			)) AS otherNames
   FROM
	    son
   GROUP BY
		son.studentuniqueid

)

select * from final