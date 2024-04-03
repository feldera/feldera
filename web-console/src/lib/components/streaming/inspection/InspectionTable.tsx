// Browse the contents of a table or a view.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { DataGridPro } from '$lib/components/common/table/DataGridProDeclarative'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { InspectionToolbar } from '$lib/components/streaming/inspection/InspectionToolbar'
import { PercentilePagination } from '$lib/components/streaming/inspection/PercentilePagination'
import { SQLTypeHeader } from '$lib/components/streaming/inspection/SQLTypeHeader'
import { SQLValueDisplay } from '$lib/components/streaming/inspection/SQLValueDisplay'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import useQuantiles from '$lib/compositions/streaming/inspection/useQuantiles'
import { useTableUpdater } from '$lib/compositions/streaming/inspection/useTableUpdater'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { useAsyncError } from '$lib/functions/common/react'
import { caseDependentNameEq, getCaseDependentName, getCaseIndependentName } from '$lib/functions/felderaRelation'
import { Row, sqlValueComparator, SQLValueJS, sqlValueToXgressJSON } from '$lib/functions/sqlValue'
import { EgressMode, Field, NeighborhoodQuery, OutputQuery, Relation } from '$lib/services/manager'
import { LS_PREFIX } from '$lib/types/localStorage'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { ReactNode, useCallback, useEffect, useState } from 'react'
import invariant from 'tiny-invariant'

import { IconButton, Tooltip } from '@mui/material'
import Card from '@mui/material/Card'
import { GridColDef, GridPaginationModel, GridRenderCellParams, GridRowSelectionModel } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

const FETCH_QUANTILES = 100

export type InspectionTableProps = {
  pipeline: Pipeline
  caseIndependentName: string
}

// The number of rows we display in the table.
const PAGE_SIZE = 25
// We are displaying only [0..PAGE_SIZE-1] rows at a time. However, we always
// fetch [-PAGE_SIZE..PAGE_SIZE] elements. This makes pagination easier: When
// next is clicked, anchor is set to rows[PAGE_SIZE]. If rows[PAGE_SIZE] does
// not exist, there is no next page. Similarly when back is clicked, anchor is
// set to the lowest genId that is less than 0. If no genId is less than 0 we
// are at the beginning of the table.
const DEFAULT_NEIGHBORHOOD: NeighborhoodQuery = { before: PAGE_SIZE, after: PAGE_SIZE }
// We don't know we're at the beginning of a table until we reach it, since the
// table might grow while we are displaying it. This isn't really supported by
// the datagrid so as a hack we just start at page X=1000 which let's us go
// backwards X times until we reach the beginning.
const INITIAL_PAGINATION_MODEL: GridPaginationModel = { pageSize: PAGE_SIZE, page: 1000 }

// We convert fields to a tuple so that we can use it as an anchor in the REST
// API.
export function rowToAnchor(relation: Relation, row: { record: Record<string, SQLValueJS> }) {
  return relation.fields.map(field => sqlValueToXgressJSON(field.columntype, row.record[field.name]))
}

// augment the props for the toolbar slot
declare module '@mui/x-data-grid-pro' {
  interface ToolbarPropsOverrides {
    pipelineName: string
    status: PipelineStatus
    relation: Relation
    isReadonly: boolean
    before: ReactNode
  }
}

/**
 * This specialized hook manages logic required for keeping rows up to date.
 * @param param
 * @returns
 */
const useInspectionTable = ({ pipeline, caseIndependentName }: InspectionTableProps) => {
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [paginationModel, setPaginationModel] = useState(INITIAL_PAGINATION_MODEL)
  const [neighborhood, setNeighborhood] = useState<NeighborhoodQuery>(DEFAULT_NEIGHBORHOOD)
  const [quantiles, setQuantiles] = useState<Record<string, SQLValueJS>[] | undefined>(undefined)
  const [quantile, setQuantile] = useState<number>(0)
  const [rows, setRows] = useState<Row[]>([])
  const [isPending, setLoading] = useState<boolean>(true)

  const { pushMessage } = useStatusNotification()
  const { updateTable, pause, resume } = useTableUpdater()

  const throwError = useAsyncError()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineRevisionQuery = useQuery(pipelineManagerQuery.pipelineLastRevision(pipeline.descriptor.name))

  // If a revision is loaded, find the requested relation that we want to
  // monitor. We use it to display the table headers etc.
  useEffect(() => {
    if (pipelineRevisionQuery.isPending || pipelineRevisionQuery.isError || !pipelineRevisionQuery.data) {
      return
    }
    setNeighborhood(DEFAULT_NEIGHBORHOOD)
    setPaginationModel(INITIAL_PAGINATION_MODEL)
    setQuantiles(undefined)
    setQuantile(0)
    setRows([])
    setLoading(true)

    const pipelineRevision = pipelineRevisionQuery.data
    const program = pipelineRevision.program
    const tables = program.schema?.inputs.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    const views = program.schema?.outputs.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    const relation = tables || views // name is unique in the schema
    if (!relation) {
      return
    }
    setRelation(relation)
  }, [pipelineRevisionQuery.isPending, pipelineRevisionQuery.isError, pipelineRevisionQuery.data, caseIndependentName])

  // Request to monitor the changes for the current rows in the table. This is
  // done by opening a stream request to the backend which then sends us the
  // initial snapshot and subsequent changes. We use setRows to update the rows
  // in the table.
  useEffect(() => {
    if (!relation) {
      return
    }
    const controller = new AbortController()
    updateTable(
      [
        pipeline.descriptor.name,
        caseIndependentName,
        'json',
        OutputQuery.NEIGHBORHOOD,
        EgressMode.WATCH,
        undefined,
        undefined,
        neighborhood
      ],
      setRows,
      setLoading,
      relation,
      controller
    ).then(
      () => {
        // nothing to do here, the tableUpdater will update the table
      },
      (error: any) => {
        if (error.name != 'AbortError') {
          throwError(error)
        } else {
          // The AbortError is expected when we leave the view
          // (controller.abort() below will trigger it)
          // -- so nothing to do here
        }
      }
    )

    return () => {
      // If we leave the view, we abort the fetch request otherwise it remains
      // active (the browser and backend only keep a limited number of active
      // requests and we don't want to exhaust that limit)
      controller.abort()
    }
  }, [pipeline, caseIndependentName, relation, updateTable, throwError, neighborhood])

  // Load quantiles for the relation we're displaying.
  const quantileLoader = useQuantiles()
  // Request from backend to monitor the changes for the current rows
  useEffect(() => {
    // The `rows.length > 0` makes sure we only send the quantile request, after
    // the neighborhood query is established. If we do it simultaneously,
    // somehow this led to an issue with sometimes not loading/displaying
    // anything at all (seems like a backend bug).
    if (!relation || rows.length === 0 || quantiles !== undefined) {
      return
    }
    const controller = new AbortController()

    quantileLoader(
      [pipeline.descriptor.name, caseIndependentName, 'json', OutputQuery.QUANTILES, EgressMode.SNAPSHOT],
      setQuantiles,
      relation,
      controller
    ).then(
      () => {
        // nothing to do here, the tableUpdater will update the table
      },
      (error: any) => {
        if (error.name != 'AbortError') {
          throwError(error)
        } else {
          // The AbortError is expected when we leave the view
          // (controller.abort() below will trigger it)
          // -- so nothing to do here
        }
      }
    )

    return () => {
      // If we leave the view, we abort the fetch request otherwise it remains
      // active (the browser and backend only keeps a limited number of active
      // requests and we don't want to exhaust that limit)
      controller.abort()
    }
  }, [pipeline, caseIndependentName, relation, quantileLoader, rows, throwError, quantiles])

  // If the user presses the previous or next button, we update the neighborhood
  // which will trigger update the rows in the table.
  const handlePaginationModelChange = useCallback(
    (newPaginationModel: GridPaginationModel) => {
      if (!relation) {
        return
      }
      if (paginationModel.page > newPaginationModel.page) {
        // Going backward, we can only go backward as long as we have at least
        // one item with a genId < 0
        const minRow = rows.find((row: any) => row.genId < 0)
        if (!minRow) {
          pushMessage({
            message: `You reached the beginning of the ${getCaseIndependentName(relation)} relation.`,
            key: new Date().getTime(),
            color: 'info'
          })
          return
        }
        setRows([])
        setNeighborhood({
          ...DEFAULT_NEIGHBORHOOD,
          anchor: rowToAnchor(relation, minRow)
        })
      } else {
        if (rows.length > 0 && rows[rows.length - 1].genId === PAGE_SIZE) {
          // Going forward, we can only go forward if we have one more item than
          // what's being displayed in the table (genId == PAGE_SIZE)
          setRows([])
          setNeighborhood({
            ...DEFAULT_NEIGHBORHOOD,
            anchor: rowToAnchor(relation, rows[rows.length - 1])
          })
        } else {
          pushMessage({
            message: `You reached the end of the ${getCaseIndependentName(relation)} relation.`,
            key: new Date().getTime(),
            color: 'info'
          })
          return
        }
      }
      setQuantile(0)
      setPaginationModel(newPaginationModel)
    },
    [rows, relation, paginationModel, pushMessage]
  )

  // If we selected a quantile from the slider, we also update the neighborhood.
  function onQuantileSelected(event: Event, value: number | number[]): void {
    // `value` is always a number since we only have one slider and we're not
    // setting a range.
    const quantileNumber = value as number
    // We map slider 1..100 to array index 0..99 because 100 values divide range
    // into 101 approx equal intervals. If the value is at 0, we just go to the
    // beginning of the table (anchor=null).
    const anchor =
      quantiles && relation && quantileNumber > 0 && quantileNumber <= 100
        ? rowToAnchor(relation, { record: quantiles[quantileNumber - 1] })
        : null
    setQuantile(value as number)
    setRows([])
    setLoading(true)
    setNeighborhood({ ...DEFAULT_NEIGHBORHOOD, anchor })
  }

  return {
    relation,
    rows,
    onQuantileSelected,
    quantile,
    setQuantile,
    quantiles,
    pipeline,
    isPending,
    handlePaginationModelChange,
    paginationModel,
    pipelineRevisionQuery,
    pause,
    resume
  }
}

export const InspectionTable = (props: InspectionTableProps) => {
  const { relation, ...data } = useInspectionTable(props)

  if (!relation) {
    return <></>
  }

  return <InspectionTableImpl {...{ relation, ...data }} />
}

/**
 * Encapsulates all logic that requires a valid Relation for consistency
 * @param param
 * @returns
 */
const InspectionTableImpl = ({
  relation,
  rows,
  onQuantileSelected,
  quantile,
  setQuantile,
  quantiles,
  pipeline,
  isPending,
  handlePaginationModelChange,
  paginationModel,
  pipelineRevisionQuery,
  pause,
  resume
}: ReturnType<typeof useInspectionTable>) => {
  invariant(relation)
  const [rowSelectionModel, setRowSelectionModel] = useState<GridRowSelectionModel>([])

  const isReadonly =
    !!pipelineRevisionQuery.data &&
    (pipelineRevisionQuery.data.program.schema?.outputs.some(caseDependentNameEq(relation)) ?? false)

  const defaultColumnVisibility = {
    genId: false,
    rowCount: false
  }
  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + `settings/streaming/inspection/${pipeline.descriptor.name}/${getCaseIndependentName(relation)}`,
    defaultColumnVisibility
  })

  return (
    <Card>
      <DataGridPro
        autoHeight
        pagination
        disableColumnFilter
        density='compact'
        getRowId={(row: Row) => row.genId}
        columns={[
          ...relation.fields.map((col: Field): GridColDef<Row> => {
            return {
              field: getCaseIndependentName(col),
              headerName: getCaseIndependentName(col),
              description: getCaseIndependentName(col),
              width: 150,
              display: 'flex',
              valueGetter: (_, row) => row.record[getCaseIndependentName(col)],
              renderHeader: () => <SQLTypeHeader col={col}></SQLTypeHeader>,
              renderCell: (props: GridRenderCellParams) => (
                <SQLValueDisplay value={props.value} type={col.columntype} />
              ),
              sortComparator: sqlValueComparator(col.columntype)
            }
          }),
          {
            field: 'genId',
            headerName: 'genId',
            description: 'Index relative to the current paginated set of rows.',
            flex: 0.5,
            valueGetter: (_, row) => row.genId,
            renderHeader: () => <></>
          },
          {
            field: 'rowCount',
            headerName: 'Row Count',
            description: 'Counts how many times this row appears in the database.',
            flex: 0.5,
            valueGetter: (_, row) => row.weight,
            renderHeader: () => <></>
          }
        ]}
        slots={{
          pagination: PercentilePagination,
          toolbar: InspectionToolbar
        }}
        slotProps={{
          pagination: {
            // @ts-ignore (this API doesn't have proper types in MUI yet)
            onQuantileSelected,
            setQuantile,
            quantile,
            // We don't show quantiles slider if we didn't fetch at least
            // `FETCH_QUANTILES` quantiles. (if we get less it means the table
            // has less than `FETCH_QUANTILES` entries and it only makes sense
            // to show the slider for larger tables).
            showSlider: quantiles && quantiles.length === FETCH_QUANTILES
          },
          toolbar: {
            printOptions: { disableToolbarButton: true },
            csvOptions: {
              fileName: getCaseIndependentName(relation),
              delimiter: ',',
              utf8WithBom: true
            },
            pipelineName: pipeline.descriptor.name,
            status: pipeline.state.current_status,
            relation: relation,
            isReadonly,
            before: [
              !!pause && (
                <Tooltip title='Pause streaming row updates' key='pause'>
                  <IconButton onClick={pause}>
                    <i className='bx bx-pause-circle' />
                  </IconButton>
                </Tooltip>
              ),
              !!resume && (
                <Tooltip title='Resume streaming row updates' key='resume'>
                  <IconButton onClick={resume}>
                    <i className='bx bx-caret-right-circle' />
                  </IconButton>
                </Tooltip>
              )
            ],
            children: [
              <ResetColumnViewButton
                key='resetColumnView'
                setColumnViewModel={gridPersistence.setColumnViewModel}
                setColumnVisibilityModel={() => gridPersistence.setColumnVisibilityModel(defaultColumnVisibility)}
              />
            ]
          },
          row: {
            'data-testid': 'box-relation-row'
          }
        }}
        loading={isPending}
        // rows will usually be bigger than PAGE_SIZE and have negative row
        // indices, so we can detect that we're at the beginning/end of the
        // table. But we always only display from 0..PAGE_SIZE-1.
        rows={rows.filter(row => row.genId >= 0 && row.genId < PAGE_SIZE)}
        paginationMode='server'
        pageSizeOptions={[PAGE_SIZE]}
        onPaginationModelChange={handlePaginationModelChange}
        paginationModel={paginationModel}
        checkboxSelection={!isReadonly}
        onRowSelectionModelChange={setRowSelectionModel}
        rowSelectionModel={rowSelectionModel}
        // Next two lines are a work-around because DataGridPro needs an
        // accurate rowCount for pagination to work which we don't have.
        //
        // This can be removed once the following issue is resolved:
        // https://github.com/mui/mui-x/issues/409
        rowCount={Number.MAX_VALUE}
        {...gridPersistence}
        sx={{
          '.MuiTablePagination-displayedRows': {
            display: 'none' // 👈 hide huge pagination number
          }
        }}
      />
    </Card>
  )
}
