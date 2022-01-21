WITH scbg as (
    select * from {{ref('raw_st_student_census_block_group')}}
    WHERE
	    scbg.studentuniqueid IS NOT NULL
	    AND scbg.txstudentcensusblockgroup IS NOT NULL
),
final as (
    SELECT
		TRIM(scbg.studentuniqueid),
		jsonb_agg(json_build_object(
				'txStudentCensusBlockGroup', TRIM(scbg.txstudentcensusblockgroup),
				'txBeginDate', TRIM(scbg.txbegindate),
				'txEndDate', TRIM(scbg.txenddate)
				)
	FROM
		scbg

)

select * from final;