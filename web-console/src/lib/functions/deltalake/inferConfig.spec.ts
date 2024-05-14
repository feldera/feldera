import { expect, test } from '@playwright/experimental-ct-react'

import { inferDeltaLakeStorageConfig } from './inferConfig'

// http://pkerschbaum.com/blog/using-playwright-to-run-unit-tests

test('Test DeltaLake config inference', async ({}) => {
  const cases: [string, any][] = [
    ['s3://bucket-0_1/pathA/pathB', { type: 'aws_s3', config: { bucket: 'bucket-0_1', endpoint: 'pathA/pathB' } }],
    ['s3a://bucket-0_1/pathA/pathB', { type: 'aws_s3', config: { bucket: 'bucket-0_1', endpoint: 'pathA/pathB' } }],
    [
      'https://s3.us-east_02.amazonaws.com/bucket-0_1',
      { type: 'aws_s3', config: { region: 'us-east_02', bucket: 'bucket-0_1' } }
    ],
    [
      'https://bucket-0_1.s3.us-east_02.amazonaws.com',
      { type: 'aws_s3', config: { bucket: 'bucket-0_1', region: 'us-east_02' } }
    ],
    [
      'https://213khgu-sdwdwqd.r2.cloudflarestorage.com/bucket-0_1',
      { type: 'aws_s3', config: { access_key_id: '213khgu-sdwdwqd', bucket: 'bucket-0_1' } }
    ],
    [
      'gs://bucket-0_1/pathA/pathB',
      { type: 'google_cloud_storage', config: { bucket: 'bucket-0_1', endpoint: 'pathA/pathB' } }
    ],
    [
      'abfss://container_0-x/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'container_0-x', endpoint: 'pathA/pathB' } }
    ],
    [
      'abfs://nt-fs_64@user_12-3.dfs.core.windows.net/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'nt-fs_64', account_name: 'user_12-3', endpoint: 'pathA/pathB' } }
    ],
    [
      'abfss://nt-fs_64@user_12-3.dfs.fabric.microsoft.com/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'nt-fs_64', account_name: 'user_12-3', endpoint: 'pathA/pathB' } }
    ],
    [
      'az://container_0-x/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'container_0-x', endpoint: 'pathA/pathB' } }
    ],
    [
      'adl://container_0-x/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'container_0-x', endpoint: 'pathA/pathB' } }
    ],
    [
      'azure://container_0-x/pathA/pathB',
      { type: 'azure_blob', config: { container_name: 'container_0-x', endpoint: 'pathA/pathB' } }
    ],
    ['https://zb-account_net.dfs.core.windows.net', { type: 'azure_blob', config: { account_name: 'zb-account_net' } }],
    [
      'https://zb-account_net.blob.core.windows.net/container_0-x',
      { type: 'azure_blob', config: { account_name: 'zb-account_net', container_name: 'container_0-x' } }
    ],
    [
      'https://zb-account_net.blob.core.windows.net',
      { type: 'azure_blob', config: { account_name: 'zb-account_net' } }
    ],
    [
      'https://zb-account_net.dfs.fabric.microsoft.com/container_0-x',
      { type: 'azure_blob', config: { account_name: 'zb-account_net', container_name: 'container_0-x' } }
    ],
    [
      'https://zb-account_net.dfs.fabric.microsoft.com',
      { type: 'azure_blob', config: { account_name: 'zb-account_net' } }
    ],
    [
      'https://zb-account_net.blob.fabric.microsoft.com/container_0-x',
      { type: 'azure_blob', config: { account_name: 'zb-account_net', container_name: 'container_0-x' } }
    ],
    [
      'https://zb-account_net.blob.fabric.microsoft.com',
      { type: 'azure_blob', config: { account_name: 'zb-account_net' } }
    ],
    ['https://regexr.com', { type: 'generic_http', config: {} }],
    ['file://home/user', { type: 'file_system', config: {} }],
    ['bbc://channel1', undefined],
    ['/home/user', { type: 'file_system', config: {} }]
  ]
  for (const pair of cases) {
    const [url, config] = pair
    await expect(inferDeltaLakeStorageConfig(url), url).toEqual(config)
  }
})
