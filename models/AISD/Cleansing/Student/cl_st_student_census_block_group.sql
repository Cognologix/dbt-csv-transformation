WITH st_student_census_block_groups AS (
	SELECT
		scbg.studentuniqueid,
		TRIM(scbg.tx_studentcensusblockgroup) AS tx_studentcensusblockgroup,
		scbg.tx_begindate,
		scbg.tx_enddate

	FROM
		{{ source('public', 'student_census_block_groups')}} AS scbg
    WHERE
	    studentuniqueid IS NOT NULL
	    AND nullif(TRIM(tx_studentcensusblockgroup),'')  IS NOT NULL
)

select * from st_student_census_block_groups
