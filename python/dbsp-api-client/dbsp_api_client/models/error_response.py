from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.error_response_details import ErrorResponseDetails


T = TypeVar("T", bound="ErrorResponse")


@define
class ErrorResponse:
    """Information returned by REST API endpoints on error.

    Attributes:
        details (ErrorResponseDetails): Detailed error metadata.
            The contents of this field is determined by `error_code`.
        error_code (str): Error code is a string that specifies this error type. Example: UnknownInputFormat.
        message (str): Human-readable error message. Example: Unknown input format 'xml'..
    """

    details: "ErrorResponseDetails"
    error_code: str
    message: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        details = self.details.to_dict()

        error_code = self.error_code
        message = self.message

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "details": details,
                "error_code": error_code,
                "message": message,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.error_response_details import ErrorResponseDetails

        d = src_dict.copy()
        details = ErrorResponseDetails.from_dict(d.pop("details"))

        error_code = d.pop("error_code")

        message = d.pop("message")

        error_response = cls(
            details=details,
            error_code=error_code,
            message=message,
        )

        error_response.additional_properties = d
        return error_response

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
