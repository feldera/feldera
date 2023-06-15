from typing import Any, Dict, List, Type, TypeVar

import attr

T = TypeVar("T", bound="SqlCompilerMessage")


@attr.s(auto_attribs=True)
class SqlCompilerMessage:
    r"""A SQL compiler error.

    The SQL compiler returns a list of errors in the following JSON format if
    it's invoked with the `-je` option.

    ```no_run
    [ {
    "startLineNumber" : 14,
    "startColumn" : 13,
    "endLineNumber" : 14,
    "endColumn" : 13,
    "warning" : false,
    "errorType" : "Error parsing SQL",
    "message" : "Encountered \"<EOF>\" at line 14, column 13."
    } ]
    ```

        Attributes:
            end_column (int):
            end_line_number (int):
            error_type (str):
            message (str):
            start_column (int):
            start_line_number (int):
            warning (bool):
    """

    end_column: int
    end_line_number: int
    error_type: str
    message: str
    start_column: int
    start_line_number: int
    warning: bool
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        end_column = self.end_column
        end_line_number = self.end_line_number
        error_type = self.error_type
        message = self.message
        start_column = self.start_column
        start_line_number = self.start_line_number
        warning = self.warning

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "endColumn": end_column,
                "endLineNumber": end_line_number,
                "errorType": error_type,
                "message": message,
                "startColumn": start_column,
                "startLineNumber": start_line_number,
                "warning": warning,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        end_column = d.pop("endColumn")

        end_line_number = d.pop("endLineNumber")

        error_type = d.pop("errorType")

        message = d.pop("message")

        start_column = d.pop("startColumn")

        start_line_number = d.pop("startLineNumber")

        warning = d.pop("warning")

        sql_compiler_message = cls(
            end_column=end_column,
            end_line_number=end_line_number,
            error_type=error_type,
            message=message,
            start_column=start_column,
            start_line_number=start_line_number,
            warning=warning,
        )

        sql_compiler_message.additional_properties = d
        return sql_compiler_message

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
