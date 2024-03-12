/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Pipeline status.
 *
 * This type represents the state of the pipeline tracked by the pipeline
 * runner and observed by the API client via the `GET /pipeline` endpoint.
 *
 * ### The lifecycle of a pipeline
 *
 * The following automaton captures the lifecycle of the pipeline.  Individual
 * states and transitions of the automaton are described below.
 *
 * * In addition to the transitions shown in the diagram, all states have an
 * implicit "forced shutdown" transition to the `Shutdown` state.  This
 * transition is triggered when the pipeline runner is unable to communicate
 * with the pipeline and thereby forces a shutdown.
 *
 * * States labeled with the hourglass symbol (⌛) are **timed** states.  The
 * automaton stays in timed state until the corresponding operation completes
 * or until the runner performs a forced shutdown of the pipeline after a
 * pre-defined timeout perioud.
 *
 * * State transitions labeled with API endpoint names (`/deploy`, `/start`,
 * `/pause`, `/shutdown`) are triggered by invoking corresponding endpoint,
 * e.g., `POST /v0/pipelines/{pipeline_id}/start`.
 *
 * ```text
 * Shutdown◄────┐
 * │         │
 * /deploy│         │
 * │   ⌛ShuttingDown
 * ▼         ▲
 * ⌛Provisioning    │
 * │         │
 * Provisioned        │         │
 * ▼         │/shutdown
 * ⌛Initializing    │
 * │         │
 * ┌────────┴─────────┴─┐
 * │        ▼           │
 * │      Paused        │
 * │      │    ▲        │
 * │/start│    │/pause  │
 * │      ▼    │        │
 * │     Running        │
 * └──────────┬─────────┘
 * │
 * ▼
 * Failed
 * ```
 *
 * ### Desired and actual status
 *
 * We use the desired state model to manage the lifecycle of a pipeline.
 * In this model, the pipeline has two status attributes associated with
 * it at runtime: the **desired** status, which represents what the user
 * would like the pipeline to do, and the **current** status, which
 * represents the actual state of the pipeline.  The pipeline runner
 * service continuously monitors both fields and steers the pipeline
 * towards the desired state specified by the user.
 * Only three of the states in the pipeline automaton above can be
 * used as desired statuses: `Paused`, `Running`, and `Shutdown`.
 * These statuses are selected by invoking REST endpoints shown
 * in the diagram.
 *
 * The user can monitor the current state of the pipeline via the
 * `/status` endpoint, which returns an object of type `Pipeline`.
 * In a typical scenario, the user first sets
 * the desired state, e.g., by invoking the `/deploy` endpoint, and
 * then polls the `GET /pipeline` endpoint to monitor the actual status
 * of the pipeline until its `state.current_status` attribute changes
 * to "paused" indicating that the pipeline has been successfully
 * initialized, or "failed", indicating an error.
 */
export enum PipelineStatus {
  SHUTDOWN = 'Shutdown',
  PROVISIONING = 'Provisioning',
  INITIALIZING = 'Initializing',
  PAUSED = 'Paused',
  RUNNING = 'Running',
  SHUTTING_DOWN = 'ShuttingDown',
  FAILED = 'Failed'
}
