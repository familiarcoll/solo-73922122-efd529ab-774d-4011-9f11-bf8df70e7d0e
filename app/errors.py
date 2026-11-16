"""Small domain-level exceptions mapped to HTTP responses in ``main``."""

from __future__ import annotations


class LakeError(Exception):
    status_code = 500
    code = "internal_error"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class ValidationError(LakeError):
    status_code = 400
    code = "invalid_input"


class NotFoundError(LakeError):
    status_code = 404
    code = "not_found"


class ConflictError(LakeError):
    status_code = 409
    code = "conflict"


class GoneError(LakeError):
    status_code = 410
    code = "gone"


class CorruptError(LakeError):
    status_code = 500
    code = "storage_inconsistent"
