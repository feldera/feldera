import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr
from dateutil.parser import isoparse

from ..models.pipeline_status import PipelineStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.error_response import ErrorResponse


T = TypeVar("T", bound="PipelineRuntimeState")


@attr.s(auto_attribs=True)
class PipelineRuntimeState:
    """Runtime state of the pipeine.

    Attributes:
        created (datetime.datetime): Time when the pipeline started executing.
        current_status (PipelineStatus): Pipeline status.

            This type represents the state of the pipeline tracked by the pipeline runner and
            observed by the API client via the `GET /pipeline` endpoint.

            ### The lifecycle of a pipeline

            The following automaton captures the lifecycle of the pipeline.  Individual states
            and transitions of the automaton are described below.

            * In addition to the transitions shown in the diagram, all states have an implicit
            "forced shutdown" transition to the `Shutdown` state.  This transition is triggered
            when the pipeline runner is unable to communicate with the pipeline and thereby
            forces a shutdown.

            * States labeled with the hourglass symbol (⌛) are **timed** states.  The automaton
            stays in timed state until the corresponding operation completes or until the runner
            performs a forced shutdown of the pipeline after a pre-defined timeout perioud.

            * State transitions labeled with API endpoint names (`/deploy`, `/start`, `/pause`,
            `/shutdown`) are triggered by invoking corresponding endpoint, e.g.,
            `POST /v0/pipelines/{pipeline_id}/start`.

            ```text
            Shutdown◄────┐
            │         │
            /deploy│         │
            │   ⌛ShuttingDown
            ▼         ▲
            ⌛Provisioning    │
            │         │
            Provisioned        │         │
            ▼         │/shutdown
            ⌛Initializing    │
            │         │
            ┌────────┴─────────┴─┐
            │        ▼           │
            │      Paused        │
            │      │    ▲        │
            │/start│    │/pause  │
            │      ▼    │        │
            │     Running        │
            └──────────┬─────────┘
            │
            ▼
            Failed
            ```

            ### Desired and actual status

            We use the desired state model to manage the lifecycle of a pipeline.
            In this model, the pipeline has two status attributes associated with
            it at runtime: the **desired** status, which represents what the user
            would like the pipeline to do, and the **current** status, which
            represents the actual state of the pipeline.  The pipeline runner
            service continuously monitors both fields and steers the pipeline
            towards the desired state specified by the user.

            Only three of the states in the pipeline automaton above can be
            used as desired statuses: `Paused`, `Running`, and `Shutdown`.
            These statuses are selected by invoking REST endpoints shown
            in the diagram.

            The user can monitor the current state of the pipeline via the
            `/status` endpoint, which returns an object of type [`Pipeline`].
            In a typical scenario, the user first sets
            the desired state, e.g., by invoking the `/deploy` endpoint, and
            then polls the `GET /pipeline` endpoint to monitor the actual status
            of the pipeline until its `state.current_status` attribute changes
            to "paused" indicating that the pipeline has been successfully
            initialized, or "failed", indicating an error.
        desired_status (PipelineStatus): Pipeline status.

            This type represents the state of the pipeline tracked by the pipeline runner and
            observed by the API client via the `GET /pipeline` endpoint.

            ### The lifecycle of a pipeline

            The following automaton captures the lifecycle of the pipeline.  Individual states
            and transitions of the automaton are described below.

            * In addition to the transitions shown in the diagram, all states have an implicit
            "forced shutdown" transition to the `Shutdown` state.  This transition is triggered
            when the pipeline runner is unable to communicate with the pipeline and thereby
            forces a shutdown.

            * States labeled with the hourglass symbol (⌛) are **timed** states.  The automaton
            stays in timed state until the corresponding operation completes or until the runner
            performs a forced shutdown of the pipeline after a pre-defined timeout perioud.

            * State transitions labeled with API endpoint names (`/deploy`, `/start`, `/pause`,
            `/shutdown`) are triggered by invoking corresponding endpoint, e.g.,
            `POST /v0/pipelines/{pipeline_id}/start`.

            ```text
            Shutdown◄────┐
            │         │
            /deploy│         │
            │   ⌛ShuttingDown
            ▼         ▲
            ⌛Provisioning    │
            │         │
            Provisioned        │         │
            ▼         │/shutdown
            ⌛Initializing    │
            │         │
            ┌────────┴─────────┴─┐
            │        ▼           │
            │      Paused        │
            │      │    ▲        │
            │/start│    │/pause  │
            │      ▼    │        │
            │     Running        │
            └──────────┬─────────┘
            │
            ▼
            Failed
            ```

            ### Desired and actual status

            We use the desired state model to manage the lifecycle of a pipeline.
            In this model, the pipeline has two status attributes associated with
            it at runtime: the **desired** status, which represents what the user
            would like the pipeline to do, and the **current** status, which
            represents the actual state of the pipeline.  The pipeline runner
            service continuously monitors both fields and steers the pipeline
            towards the desired state specified by the user.

            Only three of the states in the pipeline automaton above can be
            used as desired statuses: `Paused`, `Running`, and `Shutdown`.
            These statuses are selected by invoking REST endpoints shown
            in the diagram.

            The user can monitor the current state of the pipeline via the
            `/status` endpoint, which returns an object of type [`Pipeline`].
            In a typical scenario, the user first sets
            the desired state, e.g., by invoking the `/deploy` endpoint, and
            then polls the `GET /pipeline` endpoint to monitor the actual status
            of the pipeline until its `state.current_status` attribute changes
            to "paused" indicating that the pipeline has been successfully
            initialized, or "failed", indicating an error.
        location (str): Location where the pipeline can be reached at runtime.
            e.g., a TCP port number or a URI.
        status_since (datetime.datetime): Time when the pipeline was assigned its current status
            of the pipeline.
        error (Union[Unset, None, ErrorResponse]): Information returned by REST API endpoints on error.
    """

    created: datetime.datetime
    current_status: PipelineStatus
    desired_status: PipelineStatus
    location: str
    status_since: datetime.datetime
    error: Union[Unset, None, "ErrorResponse"] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        created = self.created.isoformat()

        current_status = self.current_status.value

        desired_status = self.desired_status.value

        location = self.location
        status_since = self.status_since.isoformat()

        error: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict() if self.error else None

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "created": created,
                "current_status": current_status,
                "desired_status": desired_status,
                "location": location,
                "status_since": status_since,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.error_response import ErrorResponse

        d = src_dict.copy()
        created = isoparse(d.pop("created"))

        current_status = PipelineStatus(d.pop("current_status"))

        desired_status = PipelineStatus(d.pop("desired_status"))

        location = d.pop("location")

        status_since = isoparse(d.pop("status_since"))

        _error = d.pop("error", UNSET)
        error: Union[Unset, None, ErrorResponse]
        if _error is None:
            error = None
        elif isinstance(_error, Unset):
            error = UNSET
        else:
            error = ErrorResponse.from_dict(_error)

        pipeline_runtime_state = cls(
            created=created,
            current_status=current_status,
            desired_status=desired_status,
            location=location,
            status_since=status_since,
            error=error,
        )

        pipeline_runtime_state.additional_properties = d
        return pipeline_runtime_state

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
