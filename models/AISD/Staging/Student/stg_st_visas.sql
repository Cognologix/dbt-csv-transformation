WITH vdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_visadescriptor')}} as vd
   on d.descriptorid = vd.visadescriptorid
),

-- Apply lookup and other business transformations at this stage
svcl as (
    select
    sv.loadid,
    sv.studentuniqueid,
    CASE
		-- When Studentvisas contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(sv.visadescriptor),'') is null)
		THEN NULL

		-- When Studentvisas is not null but does not have matching record in descriptor, set as Not Applicable category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(sv.visadescriptor),'') is not null and NULLIF(TRIM(vdm.codevalue),'') is NULL)
		THEN 'Not Applicable'

		-- Else matching record is found, so concatenate namespace and codevalue to create new visadescriptor
		else concat(vdm.namespace, '#', vdm.codevalue)

	END as visadescriptor

    from {{ref('cl_st_visas')}} as sv
    left outer join vdm
    on sv.visadescriptor = vdm.codevalue
),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		svcl.loadid as LOADID,
		svcl.studentuniqueid,
		jsonb_agg(json_build_object(
				'visaDescriptor', svcl.visadescriptor

			)) as visas
   FROM
	    svcl
   GROUP BY
		svcl.loadid,
		svcl.studentuniqueid
)

select * from final