WITH son as (
    select * from {{ref('raw_st_other_names')}}
    WHERE
	    son.studentuniqueid IS NOT NULL
	    AND son.othernametypedescriptor IS NOT NULL
	    AND son.firstname IS NOT NULL
),
final as (
   SELECT
		TRIM(son.studentuniqueid),
		jsonb_agg(json_build_object(
				'otherNameTypeDescriptor', TRIM(son.othernametypedescriptor),
				'firstName', TRIM(son.firstname),
				'generationCodeSuffix', TRIM(son.generationcodesuffix),
				'lastSurname', TRIM(son.lastsurname),
				'middleName', TRIM(son.middlename),
				'personalTitlePrefix', TRIM(son.personaltitleprefix)
			)
		)
   FROM
	    son
   GROUP BY
		son.studentuniqueid

)

select * from final;