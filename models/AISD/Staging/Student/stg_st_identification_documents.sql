WITH sid as (
    select * from {{ref('raw_st_identification_documents')}}
    WHERE
	    sid.studentuniqueid IS NOT NULL
	    AND sid.identificationdocumentusedescriptor IS NOT NULL
	    AND sid.personalinformationverificationdescriptor IS NOT NULL
)
),
final as (
    SELECT
		TRIM(sid.studentuniqueid),
		jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', TRIM(sid.identificationdocumentusedescriptor),
				'personalInformationVerificationDescriptor', TRIM(sid.personalinformationverificationdescriptor),
				'issuerCountryDescriptor', TRIM(sid.issuercountrydescriptor),
				'documentExpirationDate', TRIM(sid.documentexpirationdate),
				'documentTitle', TRIM(sid.documenttitle),
				'issuerDocumentIdentificationCode', TRIM(sid.issuerdocumentidentificationcode),
				'issuerName', TRIM(sid.issuername)
			)
	FROM
	  sid


select * from final;