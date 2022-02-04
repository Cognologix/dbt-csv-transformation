WITH idudm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_identificationdocumentusedescriptor')}} as idud
   on d.descriptorid = idud.identificationdocumentusedescriptorid
),

pivdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_personalinformationverificationdescriptor')}} as pivd
   on d.descriptorid = pivd.personalinformationverificationdescriptorid
),

-- Add lookup and other business validations at this stage
sid as (

    SELECT
        -- * from {{ref('cl_st_identification_documents')}}
    studentuniqueid,
    -- lookup identification document use descriptor
    CASE
		-- When identification document contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(spid.identificationdocumentusedescriptor),'') is null)
		THEN NULL

		-- When identification document is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(spid.identificationdocumentusedescriptor),'') is not null and NULLIF(TRIM(idudm.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new visadescriptor
		else concat(idudm.namespace, '#', idudm.codevalue)

	END as identificationdocumentusedescriptor,

    -- lookup personal info document use descriptor
    CASE
		-- When personal info document contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(spid.personalinformationverificationdescriptor),'') is null)
		THEN NULL

		-- When personal info document is not null but does not have matching record in descriptor, set as Other category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(spid.personalinformationverificationdescriptor),'') is not null and NULLIF(TRIM(pivdm.codevalue),'') is NULL)
		THEN 'Other'

		-- Else matching record is found, so concatenate namespace and codevalue to create new personalinformationverificationdescriptor
		else concat(pivdm.namespace, '#', pivdm.codevalue)

	END as personalinformationverificationdescriptor,

    spid.issuercountrydescriptor,
    spid.documentexpirationdate,
    spid.documenttitle,
    spid.issuerdocumentidentificationcode,
    spid.issuername

    from {{ref('cl_st_identification_documents')}} as spid

    left outer join idudm
    on spid.identificationdocumentusedescriptor = idudm.codevalue

    left outer join pivdm
    on spid.personalinformationverificationdescriptor = pivdm.codevalue

),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sid.studentuniqueid,
		jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', sid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', sid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', sid.issuercountrydescriptor,
				'documentExpirationDate', sid.documentexpirationdate,
				'documentTitle', sid.documenttitle,
				'issuerDocumentIdentificationCode', sid.issuerdocumentidentificationcode,
				'issuerName', sid.issuername
			)) AS identificationDocuments
	FROM
	  sid
	GROUP BY
		sid.studentuniqueid

)


select * from final