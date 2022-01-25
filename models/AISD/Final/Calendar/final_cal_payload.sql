WITH  final as (

SELECT
	c_b.resourceid,
	c_b.externalid,
	c_b.resourcetype,
	c_b.operation,
	json_build_object(
		'calendarCode', c_b.calendarcode,
		json_build_object(
		'schoolReference', c_b.schoolReference,
		'schoolYearTypeReference', c_b.schoolYearTypeReference,
		'calendarTypeDescriptor', c_b.calendartypedescriptor

	) AS payload,
	status
FROM
	{{ ref('stg_cal_base') }} AS c_b

)

select * from final
