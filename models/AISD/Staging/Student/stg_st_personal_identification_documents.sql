WITH spid as (
    select * from {{ref('raw_st_personal_identification_documents')}}
    WHERE
	    spid.studentuniqueid IS NOT NULL
	    AND spid.identificationdocumentusedescriptor IS NOT NULL
	    AND spid.personalinformationverificationdescriptor IS NOT NULL
),
final as (
   SELECT
		TRIM(spid.studentuniqueid),
        jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', TRIM(spid.identificationdocumentusedescriptor),
				'personalInformationVerificationDescriptor', TRIM(spid.personalinformationverificationdescriptor),
				'issuerCountryDescriptor', TRIM(spid.issuercountrydescriptor),
				'documentExpirationDate', TRIM(spid.documentexpirationdate),
				'documentTitle', TRIM(spid.documenttitle),
				'issuerDocumentIdentificationCode', TRIM(spid.issuerdocumentidentificationcode),
				'issuerName', TRIM(spid.issuername)
			)
		)
	FROM
	    spid
	GROUP BY
		spid.studentuniqueid

)

select * from final;