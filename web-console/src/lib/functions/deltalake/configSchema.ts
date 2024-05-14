import { FormFieldOptions } from '$lib/functions/forms'

type DeltaLakeOptions = Record<string, FormFieldOptions>

export const deltaLakeAwsOptions: DeltaLakeOptions = {
  access_key_id: { type: 'string' },
  secret_access_key: { type: 'string' },
  region: { type: 'string' },
  default_region: { type: 'string' },
  bucket: { type: 'string' },
  endpoint: { type: 'string' },
  token: { type: 'string' }
}

// export const deltaLakeAwsSchema = va.object({
//   aws_access_key_id: va.optional(va.string()),
//   aws_secret_access_key: va.optional(va.string()),
//   aws_region: va.optional(va.string()),
// })

export const deltaLakeGoogleOptions: DeltaLakeOptions = {
  service_account: { type: 'string' },
  service_account_key: { type: 'string' },
  bucket: { type: 'string' },
  application_credentials: { type: 'string' }
}

// export const deltaLakeGoogleSchema = va.object({
//   google_service_account: va.optional(va.string()),
//   google_service_account_key: va.optional(va.string()),
//   google_bucket: va.optional(va.string()),
//   google_application_credentials: va.optional(va.string())
// })

export const deltaLakeAzureOptions: DeltaLakeOptions = {
  account_name: { type: 'string' },
  access_key: { type: 'string' },
  client_id: { type: 'string' },
  client_secret: { type: 'string' },
  authority_id: { type: 'string' },
  sas_key: { type: 'string' },
  token: { type: 'string' },
  use_emulator: { type: 'boolean' },
  endpoint: { type: 'string' },
  use_fabric_endpoint: { type: 'boolean' },
  msi_endpoint: { type: 'string' },
  object_id: { type: 'string' },
  msi_resource_id: { type: 'string' },
  federated_token_file: { type: 'string' },
  use_azure_cli: { type: 'boolean' },
  skip_signature: { type: 'boolean' },
  container_name: { type: 'string' },
  disable_tagging: { type: 'boolean' }
}

// export const deltaLakeAzureSchema = va.object({
//   account_name: va.optional(va.string()),
//   access_key: va.optional(va.string()),
//   client_id,
//   client_secret,
//   authority_id,
//   sas_key,
//   token,
//   use_emulator,
//   endpoint,
//   use_fabric_endpoint,
//   msi_endpoint,
//   object_id,
//   msi_resource_id,
//   federated_token_file,
//   use_azure_cli,
//   skip_signature,
//   container_name,
//   disable_tagging,
// })

export const deltaLakeFileSystemOptions: DeltaLakeOptions = {}

export const deltaLakeGenericHttpOptions: DeltaLakeOptions = {}

export const deltaLakeNoOptions: DeltaLakeOptions = {}
