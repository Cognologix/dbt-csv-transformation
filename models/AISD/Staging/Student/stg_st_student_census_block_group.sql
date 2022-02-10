-- Apply lookup and other business transformations at this stage
WITH scbg as (
    select * from {{ref('cl_st_student_census_block_group')}}

),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		scbg.loadid as LOADID,
		scbg.studentuniqueid,
		jsonb_agg(json_build_object(
				'txStudentCensusBlockGroup', scbg.tx_studentcensusblockgroup,
				'txBeginDate', scbg.tx_begindate,
				'txEndDate', scbg.tx_enddate
				)) AS studentCensusBlockGroup
	FROM
		scbg
	GROUP BY
		scbg.loadid,
		scbg.studentuniqueid

)

select * from final