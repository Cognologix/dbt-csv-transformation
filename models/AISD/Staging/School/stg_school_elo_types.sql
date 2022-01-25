-- Apply lookup and other business transformations at this stage
WITH sc_elo_t as (

    SELECT
    	distinct schoolid,
		        tx_elotypeDescriptor,
		        tx_begindate,
		        tx_enddate
    from
        {{ref('raw_school_elo_types')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_elo_t.schoolid,
		jsonb_agg(json_build_object(
				'txELOTypeDescriptor', sc_elo_t.tx_elotypeDescriptor,
				'txBeginDate', sc_elo_t.tx_begindate,
				'txEndDate', sc_elo_t.tx_enddate
				)

			) AS eloTypes

   FROM
	    sc_elo_t
   GROUP BY
		sc_elo_t.schoolid

)

select * from final
