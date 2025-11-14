import type { FormFields } from '$lib/functions/forms'

export const deltaLakeAwsOptions: FormFields = {
  aws_access_key_id: { type: 'secret_string' },
  aws_secret_access_key: { type: 'secret_string' },
  aws_region: { type: 'string' },
  aws_default_region: { type: 'string' },
  aws_bucket: { type: 'string' },
  aws_endpoint: { type: 'string' },
  aws_token: { type: 'string' }
}

export const deltaLakeGoogleOptions: FormFields = {
  google_service_account: { type: 'string' },
  google_service_account_key: { type: 'secret_string' },
  google_bucket: { type: 'string' },
  google_application_credentials: { type: 'string' }
}

export const deltaLakeAzureOptions: FormFields = {
  azure_account_name: { type: 'string' },
  azure_access_key: { type: 'secret_string' },
  azure_client_id: { type: 'string' },
  azure_client_secret: { type: 'secret_string' },
  azure_authority_id: { type: 'string' },
  azure_sas_key: { type: 'secret_string' },
  azure_token: { type: 'string' },
  azure_use_emulator: { type: 'boolean' },
  azure_endpoint: { type: 'string' },
  azure_use_fabric_endpoint: { type: 'boolean' },
  azure_msi_endpoint: { type: 'string' },
  azure_object_id: { type: 'string' },
  azure_msi_resource_id: { type: 'string' },
  azure_federated_token_file: { type: 'string' },
  azure_use_azure_cli: { type: 'boolean' },
  azure_skip_signature: { type: 'boolean' },
  azure_container_name: { type: 'string' },
  azure_disable_tagging: { type: 'boolean' }
}

export const deltaLakeFileSystemOptions: FormFields = {}

export const deltaLakeGenericHttpOptions: FormFields = {}

export const deltaLakeNoOptions: FormFields = {}
