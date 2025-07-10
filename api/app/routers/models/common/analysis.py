from enum import Enum


DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"

class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"