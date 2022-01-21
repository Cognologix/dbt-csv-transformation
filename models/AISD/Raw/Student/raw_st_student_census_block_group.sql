WITH st_student_census_block_groups AS (
	SELECT
		scbg.studentuniqueid,
		jsonb_agg(json_build_object(
				'txStudentCensusBlockGroup', scbg.txstudentcensusblockgroup,
				'txBeginDate', scbg.txbegindate,
				'txEndDate', scbg.txenddate

			)
		) AS studentCensusBlockGroups
	FROM
		{{ source('public', '_airbyte_raw_student_census_block_groups')}} AS scbg
	GROUP BY
		scbg.studentuniqueid
)

select * from st_student_census_block_groups;
