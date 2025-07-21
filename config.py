# config.py - Centralized configuration management
import os
import json
from pathlib import Path
from typing import Optional, Dict
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Document type mapping
DOC_MAP = {"tm": "LOD", "tap": "TAP", "cc": "CERT"}

# Load state names from JSON file
def _load_state_names() -> Dict[str, str]:
    """Load state names from config file."""
    config_path = Path(__file__).parent / "config" / "state_names.json"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        # Fallback to hardcoded mapping
        return {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }

STATE_NAMES = _load_state_names()

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "require"
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create database config from environment variables."""
        return cls(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "sst"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            sslmode=os.getenv("PGSSLMODE", "require")
        )
    
    @property
    def connection_string(self) -> str:
        """Return PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"{self.database}?sslmode={self.sslmode}"
        )

@dataclass
class LoadingConfig:
    """Data loading configuration."""
    data_lake_path: Path
    log_dir: Path
    log_level: str = "INFO"
    max_retry_attempts: int = 3
    error_threshold: float = 0.10
    batch_size: int = 1000
    
    @classmethod
    def from_env(cls) -> 'LoadingConfig':
        """Create loading config from environment variables."""
        return cls(
            data_lake_path=Path(os.getenv("DATA_LAKE_PATH", r"D:\DataLake\raw\sst")),
            log_dir=Path(os.getenv("LOG_DIR", "logs")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            max_retry_attempts=int(os.getenv("MAX_RETRY_ATTEMPTS", "3")),
            error_threshold=float(os.getenv("ERROR_THRESHOLD", "0.10")),
            batch_size=int(os.getenv("BATCH_SIZE", "1000"))
        )

@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration."""
    enable_metrics: bool = True
    performance_monitoring: bool = True
    alert_on_failures: bool = True
    max_age_days: int = 365
    
    @classmethod
    def from_env(cls) -> 'MonitoringConfig':
        """Create monitoring config from environment variables."""
        return cls(
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
            performance_monitoring=os.getenv("PERFORMANCE_MONITORING", "true").lower() == "true",
            alert_on_failures=os.getenv("ALERT_ON_FAILURES", "true").lower() == "true",
            max_age_days=int(os.getenv("MAX_AGE_DAYS", "365"))
        )

class SSTConfig:
    """Main configuration class for SST project."""
    
    def __init__(self, config_file: Optional[Path] = None):
        """Initialize configuration from environment variables or config file."""
        if config_file and config_file.exists():
            # Future: Load from JSON/YAML config file
            pass
        
        self.database = DatabaseConfig.from_env()
        self.loading = LoadingConfig.from_env()
        self.monitoring = MonitoringConfig.from_env()
        
        # Create necessary directories
        self.loading.log_dir.mkdir(exist_ok=True)
        
    def validate(self) -> bool:
        """Validate configuration settings."""
        errors = []
        
        # Check database credentials
        if not self.database.password:
            errors.append("Database password not configured")
        
        # Check paths
        if not self.loading.data_lake_path.exists():
            errors.append(f"Data lake path does not exist: {self.loading.data_lake_path}")
        
        # Check numeric ranges
        if self.loading.error_threshold < 0 or self.loading.error_threshold > 1:
            errors.append("Error threshold must be between 0 and 1")
        
        if self.loading.max_retry_attempts < 1:
            errors.append("Max retry attempts must be at least 1")
        
        if errors:
            raise ValueError("Configuration validation failed:\n" + "\n".join(f"- {e}" for e in errors))
        
        return True

# Global configuration instance
config = SSTConfig()

# Convenience functions for backward compatibility
def get_connection():
    """Get database connection using global config."""
    import psycopg2
    return psycopg2.connect(
        user=config.database.user,
        password=config.database.password,
        host=config.database.host,
        port=config.database.port,
        dbname=config.database.database,
        sslmode=config.database.sslmode
    )

def get_data_lake_path() -> Path:
    """Get data lake path from config."""
    return config.loading.data_lake_path

def get_log_dir() -> Path:
    """Get log directory from config."""
    return config.loading.log_dir

def get_doc_type(folder_key: str) -> str:
    """Get document type from folder key."""
    return DOC_MAP.get(folder_key.lower(), folder_key.upper())

def get_state_name(state_code: str) -> str:
    """Get full state name from state code."""
    return STATE_NAMES.get(state_code.upper(), state_code)