WITH st_personal_identification_documents AS (
	SELECT
		spid.studentuniqueid,
        jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', spid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', spid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', spid.issuercountrydescriptor,
				'documentExpirationDate', spid.documentexpirationdate,
				'documentTitle', spid.documenttitle,
				'issuerDocumentIdentificationCode', spid.issuerdocumentidentificationcode,
				'issuerName', spid.issuername
			)
		) AS personalIdentificationDocuments
	FROM
		{{ source('public', 'student_personal_identification_documents')}} AS spid
	GROUP BY
		spid.studentuniqueid
)

select * from st_personal_identification_documents;