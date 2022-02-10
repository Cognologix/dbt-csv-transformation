{{ config(
    materialized='incremental'
    )
}}

WITH  final as (

SELECT
    c_b.loadid,
	c_b.resourceid,
	c_b.externalid,
	c_b.resourcetype,
	c_b.operation,
	now() as processed_at,

	json_build_object(
	    'calendarCode', c_b.calendarcode,
		'schoolReference', c_b.schoolReference,
		'schoolYearTypeReference', c_b.schoolYearTypeReference,
		'calendarTypeDescriptor', c_b.calendartypedescriptor

	)AS payload,
	c_b.status
FROM
	{{ ref('stg_cal_base') }} AS c_b

)

select * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})

{% endif %}