import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { PipelineRevision, PipelineStatus as RawPipelineStatus, Relation } from '$lib/services/manager'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { Provider } from '$tests/util-ct'

import { expect, test } from '@playwright/experimental-ct-react'

import { TableInspectionTab } from './TableInspectionTab'

const setTab = () => {}

test('Table Inspection Tab: test import while switching relations', async ({ mount, page }) => {
  const tab = await mount(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName1,
          relation: relation1
        }}
      />
    </Provider>
  )
  await page.getByTestId('button-add-1-random').click()
  await page.getByTestId('button-add-1-empty').click()
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName2,
          relation: relation2
        }}
      />
    </Provider>
  )
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName1,
          relation: relation1
        }}
      />
    </Provider>
  )
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName2,
          relation: relation2
        }}
      />
    </Provider>
  )
  await page.getByTestId('button-add-1-random').click()
  await page.getByTestId('button-add-1-random').click()
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName1,
          relation: relation1
        }}
      />
    </Provider>
  )
  await page.getByTestId('button-add-5-random').click()
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName2,
          relation: relation2
        }}
      />
    </Provider>
  )
  await page.getByTestId('box-data-row').first()
  await expect(tab).toHaveScreenshot('1-1.png')
  await tab.update(
    <Provider>
      <TableInspectionTab
        {...{
          tab: 'insert',
          setTab,
          pipeline,
          pipelineRevision,
          caseIndependentName: caseIndependentName1,
          relation: relation1
        }}
      />
    </Provider>
  )
  await page.getByTestId('box-data-row').first()
  await expect(tab).toHaveScreenshot('1-2.png')
})

const pipeline: Pipeline = {
  descriptor: {
    attached_connectors: undefined!,
    config: undefined!,
    description: 'Some description',
    name: 'Test Pipeline',
    pipeline_id: '00000000-0000-0000-0000-000000000000',
    program_name: 'Test Program',
    version: 12
  },
  state: {
    created: undefined!,
    current_status: PipelineStatus.RUNNING,
    desired_status: RawPipelineStatus.RUNNING,
    error: null,
    location: undefined!,
    pipeline_id: '00000000-0000-0000-0000-000000000000',
    status_since: undefined!
  }
}

const relation1: Relation = {
  name: 'table_1',
  fields: [
    {
      case_sensitive: false,
      columntype: {
        component: null,
        fields: null,
        nullable: false,
        precision: 6,
        scale: 3,
        type: 'DECIMAL'
      },
      name: 'column_a'
    },
    {
      case_sensitive: false,
      columntype: {
        component: null,
        fields: null,
        nullable: true,
        precision: 6,
        scale: null,
        type: 'VARCHAR'
      },
      name: 'COLUMN_B'
    },
    {
      case_sensitive: true,
      columntype: {
        component: null,
        fields: null,
        nullable: false,
        precision: null,
        scale: null,
        type: 'BOOLEAN'
      },
      name: 'column_c'
    },
    {
      case_sensitive: true,
      columntype: {
        component: null,
        fields: null,
        nullable: true,
        precision: null,
        scale: null,
        type: 'TIMESTAMP'
      },
      name: 'COLUMN_D'
    }
  ],
  case_sensitive: false
}

const relation2: Relation = {
  name: 'table_2',
  fields: [
    {
      case_sensitive: false,
      columntype: {
        component: null,
        fields: null,
        nullable: false,
        precision: null,
        scale: null,
        type: 'CHAR'
      },
      name: 'column_e'
    },
    {
      case_sensitive: false,
      columntype: {
        component: null,
        fields: null,
        nullable: true,
        precision: null,
        scale: null,
        type: 'INTEGER'
      },
      name: 'COLUMN_B'
    },
    {
      case_sensitive: true,
      columntype: {
        component: null,
        fields: null,
        nullable: false,
        precision: null,
        scale: null,
        type: 'TIME'
      },
      name: 'column_c'
    }
  ],
  case_sensitive: false
}

const pipelineRevision: PipelineRevision = {
  config: undefined!,
  connectors: undefined!,
  pipeline: pipeline.descriptor,
  program: {
    code: 'DROP TABLE users;',
    config: undefined!,
    description: undefined!,
    name: 'Test Program',
    program_id: '00000000-0000-0000-0000-000000000000',
    schema: undefined!,
    status: undefined!,
    version: undefined!
  },
  revision: '8',
  services_for_connectors: undefined!
}

const caseIndependentName1 = getCaseIndependentName(relation1)
const caseIndependentName2 = getCaseIndependentName(relation2)
