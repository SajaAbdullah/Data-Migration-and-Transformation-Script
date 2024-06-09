from enum import Enum


class Status(Enum):
    DRAFT = "Draft"
    READY_FOR_REVIEW = "ReadyForReview"
    ON_PROD = "OnProd"
    EDITS_REQUIRED = "EditsRequired"


STATUS_CHOICES = (
    "Draft",
    "ReadyForReview",
    "OnProd",
    "EditsRequired",
)
