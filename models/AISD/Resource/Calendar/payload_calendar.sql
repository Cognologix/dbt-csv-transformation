{{ config(
    materialized='table'
    )
}}

WITH  final as (

SELECT
	c_b.resourceid,
	c_b.externalid,
	c_b.resourcetype,
	c_b.operation,

    CURRENT_TIME as modified_at,

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
  where modified_at > (select max(modified_at) from {{ this }})

{% endif %}