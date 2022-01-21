WITH st_other_names AS (
	SELECT
		son.studentuniqueid,
		jsonb_agg(json_build_object(
				'otherNameTypeDescriptor', son.othernametypedescriptor,
				'firstName', son.firstname,
				'generationCodeSuffix', son.generationcodesuffix,
				'lastSurname', son.lastsurname,
				'middleName', son.middlename,
				'personalTitlePrefix', son.personaltitleprefix
			)
		) AS otherNames
	FROM
		{{ source('public', 'student_other_names')}} AS son
	GROUP BY
		son.studentuniqueid
)

select * from st_other_names;