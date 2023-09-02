// Browse the contents of a table or a view.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { InspectionToolbar } from '$lib/components/streaming/inspection/InspectionToolbar'
import { PercentilePagination } from '$lib/components/streaming/inspection/PercentilePagination'
import useQuantiles from '$lib/compositions/streaming/inspection/useQuantiles'
import useTableUpdater from '$lib/compositions/streaming/inspection/useTableUpdater'
import { useAsyncError } from '$lib/functions/common/react'
import { Row, rowToAnchor } from '$lib/functions/ddl'
import { NeighborhoodQuery, OpenAPI, Pipeline, Relation } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { useCallback, useEffect, useState } from 'react'

import Card from '@mui/material/Card'
import { DataGridPro, GridPaginationModel, GridRowSelectionModel } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

const FETCH_QUANTILES = 100

export type InspectionTableProps = {
  pipeline: Pipeline
  name: string
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

export const InspectionTable = ({ pipeline, name }: InspectionTableProps) => {
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [paginationModel, setPaginationModel] = useState(INITIAL_PAGINATION_MODEL)
  const [neighborhood, setNeighborhood] = useState<NeighborhoodQuery>(DEFAULT_NEIGHBORHOOD)
  const [quantiles, setQuantiles] = useState<any[][] | undefined>(undefined)
  const [quantile, setQuantile] = useState<number>(0)
  const [rows, setRows] = useState<Row[]>([])
  const [isLoading, setLoading] = useState<boolean>(true)

  const { pushMessage } = useStatusNotification()
  const tableUpdater = useTableUpdater()

  const throwError = useAsyncError()
  const pipelineRevisionQuery = useQuery(PipelineManagerQuery.pipelineLastRevision(pipeline?.descriptor.pipeline_id))

  // If a revision is loaded, find the requested relation that we want to
  // monitor. We use it to display the table headers etc.
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data) {
      setNeighborhood(DEFAULT_NEIGHBORHOOD)
      setPaginationModel(INITIAL_PAGINATION_MODEL)
      setQuantiles(undefined)
      setQuantile(0)
      setRows([])
      setLoading(true)

      const pipelineRevision = pipelineRevisionQuery.data
      const program = pipelineRevision.program
      const tables = program.schema?.inputs.find(v => v.name === name)
      const views = program.schema?.outputs.find(v => v.name === name)
      const relation = tables || views // name is unique in the schema
      if (!relation) {
        return
      }
      setRelation(relation)
    }
  }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data, name])

  // Request to monitor the changes for the current rows in the table. This is
  // done by opening a stream request to the backend which then sends us the
  // initial snapshot and subsequent changes. We use setRows to update the rows
  // in the table.
  useEffect(() => {
    if (relation !== undefined) {
      const controller = new AbortController()
      const url = new URL(
        OpenAPI.BASE +
          '/pipelines/' +
          pipeline.descriptor.pipeline_id +
          '/egress/' +
          name +
          '?format=csv&query=neighborhood&mode=watch'
      )
      tableUpdater(url, neighborhood, setRows, setLoading, relation, controller).then(
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
    }
  }, [pipeline, name, relation, tableUpdater, throwError, neighborhood])

  // Load quantiles for the relation we're displaying.
  const quantileLoader = useQuantiles()
  // Request from backend to monitor the changes for the current rows
  useEffect(() => {
    // The `rows.length > 0` makes sure we only send the quantile request, after
    // the neighborhood query is established. If we do it simultaneously,
    // somehow this led to an issue with sometimes not loading/displaying
    // anything at all (seems like a backend bug).
    if (relation !== undefined && rows.length > 0 && quantiles === undefined) {
      const controller = new AbortController()
      const url = new URL(
        OpenAPI.BASE +
          '/pipelines/' +
          pipeline.descriptor.pipeline_id +
          '/egress/' +
          name +
          '?format=csv&query=quantiles&mode=snapshot'
      )
      quantileLoader(url, setQuantiles, relation, controller).then(
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
    }
  }, [pipeline, name, relation, quantileLoader, rows, throwError, quantiles])

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
            message: `You reached the beginning of the ${relation.name} relation.`,
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
            message: `You reached the end of the ${relation.name} relation.`,
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
    const anchor = quantiles && quantileNumber > 0 && quantileNumber <= 100 ? quantiles[quantileNumber - 1] : null
    setQuantile(value as number)
    setRows([])
    setLoading(true)
    setNeighborhood({ ...DEFAULT_NEIGHBORHOOD, anchor })
  }

  const [rowSelectionModel, setRowSelectionModel] = useState<GridRowSelectionModel>([])

  return relation ? (
    <Card>
      <DataGridPro
        autoHeight
        pagination
        disableColumnFilter
        density='compact'
        getRowId={(row: Row) => row.genId}
        initialState={{
          columns: {
            columnVisibilityModel: {
              genId: false,
              rowCount: false
            }
          }
        }}
        columns={relation.fields
          .map((col: any) => {
            return {
              field: col.name,
              headerName: col.name,
              description: col.name,
              flex: 1,
              valueGetter: (params: any) => params.row.record[col.name]
            }
          })
          .concat([
            {
              field: 'genId',
              headerName: 'genId',
              description: 'Index relative to the current paginated set of rows.',
              flex: 0.5,
              valueGetter: (params: any) => params.row.genId
            },
            {
              field: 'rowCount',
              headerName: 'Row Count',
              description: 'Counts how many times this row appears in the database.',
              flex: 0.5,
              valueGetter: (params: any) => params.row.weight
            }
          ])}
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
              fileName: relation.name,
              delimiter: ',',
              utf8WithBom: true
            },
            pipelineId: pipeline.descriptor.pipeline_id,
            status: pipeline.state.current_status,
            relation: relation.name
          }
        }}
        loading={isLoading}
        // rows will usually be bigger than PAGE_SIZE and have negative row
        // indices, so we can detect that we're at the beginning/end of the
        // table. But we always only display from 0..PAGE_SIZE-1.
        rows={rows.filter((row: any) => row.genId >= 0 && row.genId < PAGE_SIZE)}
        paginationMode='server'
        pageSizeOptions={[PAGE_SIZE]}
        onPaginationModelChange={model => handlePaginationModelChange(model)}
        paginationModel={paginationModel}
        checkboxSelection
        onRowSelectionModelChange={newRowSelectionModel => {
          setRowSelectionModel(newRowSelectionModel)
        }}
        rowSelectionModel={rowSelectionModel}
        // Next two lines are a work-around because DataGridPro needs an
        // accurate rowCount for pagination to work which we don't have.
        //
        // This can be removed once the following issue is resolved:
        // https://github.com/mui/mui-x/issues/409
        rowCount={Number.MAX_VALUE}
        sx={{
          '.MuiTablePagination-displayedRows': {
            display: 'none' // 👈 hide huge pagination number
          }
        }}
      />
    </Card>
  ) : (
    <></>
  )
}
