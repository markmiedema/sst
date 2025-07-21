from dataclasses import dataclass

@dataclass
class ETLConfig:
    error_threshold: float = 0.10
    parallel_workers: int = 4
    retry_attempts: int = 3
    retry_backoff_ms: int = 1000
