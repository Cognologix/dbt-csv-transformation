WITH sc_txetd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_elotypedescriptor')}} as src_txetd
   on d.descriptorid = src_txetd.tx_elotypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_elo_t as (

    SELECT
        -- * from {{ref('cl_school_elo_types')}}
    loadid,
    schoolid,
    -- lookup tx_elo Activity Descriptor use descriptor
    CASE
		-- When tx_elo Activity Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_set.tx_elotypedescriptor),'') is null)
		THEN NULL

		-- When tx_elo Activity Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_set.tx_elotypedescriptor),'') is not null and NULLIF(TRIM(sc_txetd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new tx_elo Activity Descriptor
		else concat(sc_txetd.namespace, '#', sc_txetd.codevalue)

	END as tx_elotypedescriptor,
	tx_begindate,
	tx_enddate


    from {{ref('cl_school_elo_types')}} as cl_set

    left outer join sc_txetd
    on cl_set.tx_elotypedescriptor = sc_txetd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------

final as (
   SELECT
        sc_elo_t.loadid as LOADID,
		sc_elo_t.schoolid,
		jsonb_agg(json_build_object(
				'txELOTypeDescriptor', sc_elo_t.tx_elotypedescriptor,
				'txBeginDate', sc_elo_t.tx_begindate,
				'txEndDate', sc_elo_t.tx_enddate,
				'eloActivities', sc_ea.eloActivities
				)

			) AS eloTypes

   FROM
	    sc_elo_t
   LEFT JOIN
        {{ref('stg_school_elo_activities')}} AS sc_ea
    ON
        sc_ea.schoolid = sc_elo_t.schoolid

   GROUP BY
        sc_elo_t.loadid,
		sc_elo_t.schoolid

)

select * from final
