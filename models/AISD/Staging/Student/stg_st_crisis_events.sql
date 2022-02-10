-- Add lookup and other business validations at this stage
--WITH sce as (
--
--    SELECT * from {{ref('cl_st_crisis_events')}}
--
--),
WITH ced as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_crisiseventdescriptor')}} as src_ced
   on d.descriptorid = src_ced.tx_crisiseventdescriptorid
),

sce as (

    SELECT
    	distinct
    	loadid,
    	studentuniqueid,
        CASE
        -- When tx_crisiseventdescriptor contain blanks or null, process as is or set null
        -- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
        when (NULLIF(TRIM(cl_ced.tx_crisiseventdescriptor),'') is null)
        THEN NULL

        -- When tx_crisiseventdescriptor is not null but does not have matching record in descriptor, set as BLANK/NULL
        -- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
        when (NULLIF(TRIM(cl_ced.tx_crisiseventdescriptor),'') is not null and NULLIF(TRIM(ced.codevalue),'') is NULL)
        THEN ''

        -- Else matching record is found, so concatenate namespace and codevalue to create new tx_crisiseventdescriptor
        else concat(ced.namespace, '#', ced.codevalue)

        END as tx_crisiseventdescriptor,
        tx_begindate,
        tx_enddate
    FROM
        {{ref('cl_st_crisis_events')}} as cl_ced
        left outer join ced
        on cl_ced.tx_crisiseventdescriptor = ced.codevalue
),
-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sce.loadid as LOADID,
		sce.studentuniqueid,
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', sce.tx_crisiseventdescriptor,
				'txBeginDate', sce.tx_begindate,
				'txEndDate', sce.tx_enddate
			)) as crisisEvents
	FROM
	  sce
    GROUP BY
		sce.loadid,
		sce.studentuniqueid
)

select * from final