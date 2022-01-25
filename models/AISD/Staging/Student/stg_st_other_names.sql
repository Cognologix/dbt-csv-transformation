WITH ondm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_othernametypedescriptor')}} as ond
   on d.descriptorid = ond.othernametypedescriptorid
),

-- Apply lookup and other business transformations at this stage
son as (

    SELECT
    	distinct studentuniqueid,

        CASE
        -- When othernametypedescriptor contain blanks or null, process as is or set null
        -- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
        when (NULLIF(TRIM(othernametypedescriptor),'') is null)
        THEN NULL

        -- When othernametypedescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
        when (NULLIF(TRIM(othernametypedescriptor),'') is not null and NULLIF(TRIM(ondm.codevalue),'') is NULL)
        THEN 'Other Name'

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(ondm.namespace, '#', ondm.codevalue)

        END as othernametypedescriptor,

		firstname,
		generationcodesuffix,
		lastsurname,
		middlename,
		personaltitleprefix
    from
        {{ref('raw_st_other_names')}}
    left outer join ondm
    on othernametypedescriptor = ondm.codevalue

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		son.studentuniqueid,
		jsonb_agg(json_build_object(
				'otherNameTypeDescriptor', son.othernametypedescriptor,
				'firstName', son.firstname,
				'generationCodeSuffix', son.generationcodesuffix,
				'lastSurname', son.lastsurname,
				'middleName', son.middlename,
				'personalTitlePrefix', son.personaltitleprefix
			)) AS otherNames
   FROM
	    son
   GROUP BY
		son.studentuniqueid

)

select * from final