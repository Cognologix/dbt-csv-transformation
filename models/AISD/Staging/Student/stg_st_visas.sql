-- Apply lookup and other business transformations at this stage
WITH sv as (
    select * from {{ref('raw_st_visas')}}

),

dv as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_visadescriptor')}} as vd
   on d.descriptorid = vd.visadescriptorid
),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sv.studentuniqueid,
		jsonb_agg(json_build_object(
				'visaDescriptor', sv.visadescriptor

			)) as visas
   FROM
	    sv

   JOIN  dv
	on sv.visadescriptor = concat (dv.namespace, '#', dv.codevalue)

   GROUP BY
		sv.studentuniqueid
)

select * from final