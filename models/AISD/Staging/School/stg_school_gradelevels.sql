WITH sc_gld as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_gradeleveldescriptor')}} as src_gld
   on d.descriptorid = src_gld.gradeleveldescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_gl as (

    SELECT
        -- * from {{ref('cl_school_gradelevels')}}
    schoolid,
    -- lookup Grade Level Descriptor use descriptor
    CASE
		-- When Grade Level Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sgl.gradeleveldescriptor),'') is null)
		THEN NULL

		-- When Grade Level Descriptor is not null but does not have matching record in descriptor, set as BLANK category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sgl.gradeleveldescriptor),'') is not null and NULLIF(TRIM(sc_gld.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Grade Level Descriptor
		else concat(sc_gld.namespace, '#', sc_gld.codevalue)

	END as gradeleveldescriptor

    from {{ref('cl_school_gradelevels')}} as cl_sgl

    left outer join sc_gld
    on cl_sgl.gradeleveldescriptor = sc_gld.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
		sc_gl.schoolid,
		jsonb_agg(json_build_object(
				'gradeLevelDescriptor', sc_gl.gradeleveldescriptor

        )) AS gradeLevels
   FROM
	    sc_gl
   GROUP BY
		sc_gl.schoolid

)

select * from final
