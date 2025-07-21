# loader/logging_config.py
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime

def setup_logging(log_dir: Path = Path("logs"), log_level: str = "INFO"):
    """
    Set up comprehensive logging with file rotation and console output.
    """
    log_dir.mkdir(exist_ok=True)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler for INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    # File handler for all logs with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / f"sst_loader_{datetime.now():%Y%m%d}.log",
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Error file handler for ERROR and above
    error_handler = logging.handlers.RotatingFileHandler(
        log_dir / "sst_loader_errors.log",
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    
    # Add handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)
    
    # Create specialized loggers
    loggers = {
        'loader': logging.getLogger('sst.loader'),
        'parser': logging.getLogger('sst.parser'),
        'db': logging.getLogger('sst.db'),
        'validation': logging.getLogger('sst.validation')
    }
    
    return loggers

# Usage in your loader
class EnhancedSSTDatabaseLoader:
    def __init__(self, conn, logger=None):
        self.conn = conn
        self.logger = logger or logging.getLogger('sst.loader')
        
    def load_combined(self, csv_path: Path, doc_type: str, 
                      state_code: str, state_name: str, version_hint: str):
        self.logger.info(f"Starting load: {doc_type} for {state_code} v{version_hint}")
        
        try:
            # ... existing code ...
            self.logger.debug(f"Reading file: {csv_path}")
            raw_data = csv_path.read_text(encoding="utf-8-sig")
            
            # ... rest of method ...
            
            self.logger.info(f"Successfully loaded {doc_type} for {state_code}")
            
        except Exception as e:
            self.logger.error(
                f"Failed to load {doc_type} for {state_code}: {e}",
                exc_info=True
            )
            raise