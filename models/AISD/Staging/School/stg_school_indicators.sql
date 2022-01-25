-- Apply lookup and other business transformations at this stage
WITH sc_ind as (

    SELECT
    	distinct schoolid,
		        indicatordescriptor,
		        indicatorgroupdescriptor,
		        indicatorleveldescriptor,
		        designatedby,
		        indicatorvalue,
		        begindate,
		        enddate
    from
        {{ref('raw_school_indicators')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_ind.schoolid,
		jsonb_agg(json_build_object(
				'indicatorDescriptor', sc_ind.indicatordescriptor,
				'indicatorGroupDescriptor', sc_ind.indicatorgroupdescriptor,
				'indicatorLevelDescriptor', sc_ind.indicatorleveldescriptor,
				'designatedBy', sc_ind.designatedby,
                'indicatorValue', sc_ind.indicatorvalue,
                )

			) AS indicators,
	    sc_ind.begindate,
		sc_ind.enddate

   FROM
	    sc_ind
   GROUP BY
		sc_ind.schoolid

)

select * from final

