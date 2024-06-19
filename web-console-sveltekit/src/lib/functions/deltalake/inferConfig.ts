export type DeltaLakeStorageType =
  | 'aws_s3'
  | 'google_cloud_storage'
  | 'azure_blob'
  | 'generic_http'
  | 'file_system'

/**
 *
 * @see https://github.com/fsspec/adlfs
 */
export const inferDeltaLakeStorageConfig = (uri: string) => {
  const patterns: [
    RegExp,
    (
      uri: string[]
    ) =>
      | { type: DeltaLakeStorageType; config: Record<string, string | number | boolean> }
      | undefined
  ][] = [
    [
      /s3:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, aws_bucket, aws_endpoint]) => ({ type: 'aws_s3', config: { aws_bucket, aws_endpoint } })
    ],
    [
      /s3a:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, aws_bucket, aws_endpoint]) => ({ type: 'aws_s3', config: { aws_bucket, aws_endpoint } })
    ],
    [
      /https:\/\/s3.([\w_-]+).amazonaws.com\/([\w_-]+)/,
      ([_, aws_region, aws_bucket]) => ({ type: 'aws_s3', config: { aws_region, aws_bucket } })
    ],
    [
      /https:\/\/([\w_-]+).s3.([\w_-]+).amazonaws.com/,
      ([_, aws_bucket, aws_region]) => ({ type: 'aws_s3', config: { aws_bucket, aws_region } })
    ],
    [
      /https:\/\/([\w_-]+).r2.cloudflarestorage.com\/([\w_-]+)/,
      ([_, aws_access_key_id, aws_bucket]) => ({
        type: 'aws_s3',
        config: { aws_access_key_id, aws_bucket }
      })
    ],
    [
      /gs:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, google_bucket, google_endpoint]) => ({
        type: 'google_cloud_storage',
        config: { google_bucket, google_endpoint }
      })
    ],
    [
      /abfss?:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_endpoint }
      })
    ],
    [
      /abfss?:\/\/([\w_-]+)@([\w_-]+).dfs.core.windows.net\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_account_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_account_name, azure_endpoint }
      })
    ],
    [
      /abfss?:\/\/([\w_-]+)@([\w_-]+).dfs.fabric.microsoft.com\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_account_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_account_name, azure_endpoint }
      })
    ],
    [
      /az:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_endpoint }
      })
    ],
    [
      /adl:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_endpoint }
      })
    ],
    [
      /azure:\/\/([\w_-]+)\/([\w/_-]+)?/,
      ([_, azure_container_name, azure_endpoint]) => ({
        type: 'azure_blob',
        config: { azure_container_name, azure_endpoint }
      })
    ],
    [
      /https:\/\/([\w_-]+).dfs.core.windows.net/,
      ([_, azure_account_name]) => ({ type: 'azure_blob', config: { azure_account_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.core.windows.net\/([\w_-]+)/,
      ([_, azure_account_name, azure_container_name]) => ({
        type: 'azure_blob',
        config: { azure_account_name, azure_container_name }
      })
    ],
    [
      /https:\/\/([\w_-]+).blob.core.windows.net/,
      ([_, azure_account_name]) => ({ type: 'azure_blob', config: { azure_account_name } })
    ],
    [
      /https:\/\/([\w_-]+).dfs.fabric.microsoft.com\/([\w_-]+)/,
      ([_, azure_account_name, azure_container_name]) => ({
        type: 'azure_blob',
        config: { azure_account_name, azure_container_name }
      })
    ],
    [
      /https:\/\/([\w_-]+).dfs.fabric.microsoft.com/,
      ([_, azure_account_name]) => ({ type: 'azure_blob', config: { azure_account_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.fabric.microsoft.com\/([\w_-]+)/,
      ([_, azure_account_name, azure_container_name]) => ({
        type: 'azure_blob',
        config: { azure_account_name, azure_container_name }
      })
    ],
    [
      /https:\/\/([\w_-]+).blob.fabric.microsoft.com/,
      ([_, azure_account_name]) => ({ type: 'azure_blob', config: { azure_account_name } })
    ],
    [/http[s]?:\/\//, () => ({ type: 'generic_http', config: {} })],
    [/file:\/\//, () => ({ type: 'file_system', config: {} })],
    [/.+(?:\/\/)/, () => undefined],
    [/.+/, () => ({ type: 'file_system', config: {} })]
  ]
  const pattern = patterns.find(([regex]) => regex.exec(uri))
  return pattern ? (([regex, config]) => config?.(regex.exec(uri)!))(pattern) : undefined
}
