WITH st_student_census_block_groups AS (
	SELECT
		{{ var('LOADID',-1) }} as LOADID,
		scbg.studentuniqueid,
		TRIM(scbg.tx_studentcensusblockgroup) AS tx_studentcensusblockgroup,
		scbg.tx_begindate,
		scbg.tx_enddate

	FROM
		{{ source('raw_data', 'student_census_block_groups')}} AS scbg
    WHERE
	    studentuniqueid IS NOT NULL
	    AND nullif(TRIM(tx_studentcensusblockgroup),'')  IS NOT NULL
)

select * from st_student_census_block_groups
