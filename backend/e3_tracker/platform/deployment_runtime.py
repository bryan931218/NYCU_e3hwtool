"""Compose study deployment behavior with the shared database infrastructure."""

from e3_tracker.platform.storage import PersistentStorage
from e3_tracker.study.persistence.deployment import StudyDeploymentStorage


class DeploymentSafeStorage(StudyDeploymentStorage, PersistentStorage):
    """Combined repository used by the existing deployment."""
