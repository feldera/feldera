class CheckpointMetadata:
    def __init__(
        self,
        uuid: str,
        size: int,
        steps: int,
        processed_records: int,
        fingerprint: int,
        identifier: str | None = None,
    ):
        self.uuid = uuid
        self.size = size
        self.steps = steps
        self.processed_records = processed_records
        self.fingerprint = fingerprint
        self.identifier = identifier

    @classmethod
    def from_dict(self, chk_dict: dict):
        return CheckpointMetadata(
            uuid=chk_dict["uuid"],
            size=chk_dict["size"],
            steps=chk_dict["steps"],
            processed_records=chk_dict["processed_records"],
            fingerprint=chk_dict["fingerprint"],
            identifier=chk_dict.get("identifier"),
        )

    def to_dict(self) -> dict:
        chk_dict = {
            "uuid": self.uuid,
            "size": self.size,
            "steps": self.steps,
            "processed_records": self.processed_records,
            "fingerprint": self.fingerprint,
        }
        if self.identifier is not None:
            chk_dict["identifier"] = self.identifier
        return chk_dict
