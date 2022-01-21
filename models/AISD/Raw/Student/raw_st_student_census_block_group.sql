WITH st_student_census_block_groups AS (
	SELECT
		scbg.studentuniqueid,
		jsonb_agg(json_build_object(
				'txStudentCensusBlockGroup', scbg.tx_studentcensusblockgroup,
				'txBeginDate', scbg.tx_begindate,
				'txEndDate', scbg.tx_enddate

			)
		) AS studentCensusBlockGroups
	FROM
		{{ source('public', 'student_census_block_groups')}} AS scbg
	GROUP BY
		scbg.studentuniqueid
)

select * from st_student_census_block_groups;
