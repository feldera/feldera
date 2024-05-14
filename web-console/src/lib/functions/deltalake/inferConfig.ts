export type DeltaLakeStorageType = 'aws_s3' | 'google_cloud_storage' | 'azure_blob' | 'generic_http' | 'file_system'

/**
 *
 * @see https://github.com/fsspec/adlfs
 */
export const inferDeltaLakeStorageConfig = (uri: string) => {
  const patterns: [
    RegExp,
    (uri: string[]) => { type: DeltaLakeStorageType; config: Record<string, string | number | boolean> } | undefined
  ][] = [
    [/s3:\/\/([\w_-]+)\/([\w/_-]+)/, ([_, bucket, endpoint]) => ({ type: 'aws_s3', config: { bucket, endpoint } })],
    [/s3a:\/\/([\w_-]+)\/([\w/_-]+)/, ([_, bucket, endpoint]) => ({ type: 'aws_s3', config: { bucket, endpoint } })],
    [
      /https:\/\/s3.([\w_-]+).amazonaws.com\/([\w_-]+)/,
      ([_, region, bucket]) => ({ type: 'aws_s3', config: { region, bucket } })
    ],
    [
      /https:\/\/([\w_-]+).s3.([\w_-]+).amazonaws.com/,
      ([_, bucket, region]) => ({ type: 'aws_s3', config: { bucket, region } })
    ],
    [
      /https:\/\/([\w_-]+).r2.cloudflarestorage.com\/([\w_-]+)/,
      ([_, access_key_id, bucket]) => ({ type: 'aws_s3', config: { access_key_id, bucket } })
    ],
    [
      /gs:\/\/([\w_-]+)\/([\w/_-]+)/,
      ([_, bucket, endpoint]) => ({ type: 'google_cloud_storage', config: { bucket, endpoint } })
    ],
    [
      /abfss?:\/\/([\w_-]+)\/([\w/_-]+)/,
      ([_, container_name, endpoint]) => ({ type: 'azure_blob', config: { container_name, endpoint } })
    ],
    [
      /abfss?:\/\/([\w_-]+)@([\w_-]+).dfs.core.windows.net\/([\w/_-]+)/,
      ([_, container_name, account_name, endpoint]) => ({
        type: 'azure_blob',
        config: { container_name, account_name, endpoint }
      })
    ],
    [
      /abfss?:\/\/([\w_-]+)@([\w_-]+).dfs.fabric.microsoft.com\/([\w/_-]+)/,
      ([_, container_name, account_name, endpoint]) => ({
        type: 'azure_blob',
        config: { container_name, account_name, endpoint }
      })
    ],
    [
      /az:\/\/([\w_-]+)\/([\w/_-]+)/,
      ([_, container_name, endpoint]) => ({ type: 'azure_blob', config: { container_name, endpoint } })
    ],
    [
      /adl:\/\/([\w_-]+)\/([\w/_-]+)/,
      ([_, container_name, endpoint]) => ({ type: 'azure_blob', config: { container_name, endpoint } })
    ],
    [
      /azure:\/\/([\w_-]+)\/([\w/_-]+)/,
      ([_, container_name, endpoint]) => ({ type: 'azure_blob', config: { container_name, endpoint } })
    ],
    [
      /https:\/\/([\w_-]+).dfs.core.windows.net/,
      ([_, account_name]) => ({ type: 'azure_blob', config: { account_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.core.windows.net\/([\w_-]+)/,
      ([_, account_name, container_name]) => ({ type: 'azure_blob', config: { account_name, container_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.core.windows.net/,
      ([_, account_name]) => ({ type: 'azure_blob', config: { account_name } })
    ],
    [
      /https:\/\/([\w_-]+).dfs.fabric.microsoft.com\/([\w_-]+)/,
      ([_, account_name, container_name]) => ({ type: 'azure_blob', config: { account_name, container_name } })
    ],
    [
      /https:\/\/([\w_-]+).dfs.fabric.microsoft.com/,
      ([_, account_name]) => ({ type: 'azure_blob', config: { account_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.fabric.microsoft.com\/([\w_-]+)/,
      ([_, account_name, container_name]) => ({ type: 'azure_blob', config: { account_name, container_name } })
    ],
    [
      /https:\/\/([\w_-]+).blob.fabric.microsoft.com/,
      ([_, account_name]) => ({ type: 'azure_blob', config: { account_name } })
    ],
    [/http[s]?:\/\//, () => ({ type: 'generic_http', config: {} })],
    [/file:\/\//, () => ({ type: 'file_system', config: {} })],
    [/.+(?:\/\/)/, () => undefined],
    [/.+/, () => ({ type: 'file_system', config: {} })]
  ]
  const pattern = patterns.find(([regex]) => regex.exec(uri))
  return pattern ? (([regex, config]) => config?.(regex.exec(uri)!))(pattern) : undefined
}
