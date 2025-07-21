from tenacity import retry, stop_after_attempt, wait_exponential
from .config import ETLConfig

def with_retry(fn):
    return retry(
        stop=stop_after_attempt(ETLConfig.retry_attempts),
        wait=wait_exponential(multiplier=ETLConfig.retry_backoff_ms / 1000),
    )(fn)
