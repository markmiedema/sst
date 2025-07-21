# SST (Streamlined Sales Tax) Project

This project is a data pipeline system for loading, validating, and managing tax compliance documents from U.S. states. It processes three types of documents: LOD (Library of Definitions), CERT (Certificate of Compliance), and TAP (Tax Administration Practices).

## Architecture Overview

- **Main Entry Points**: `main.py` (single file), `bulk_load_all.py` (bulk processing)
- **Parsers**: Multi-format document parsers (CSV, JSON, PDF) with error recovery
- **Loader**: Database loading with validation, change detection, and retry logic
- **Monitoring**: Performance monitoring and recovery capabilities
- **Database**: PostgreSQL with temporal data model and comprehensive indexing

## Key Commands

### Testing
```bash
python -m pytest tests/
```

### Database Connection Test
```bash
python test_connection.py
```

### Single File Processing
```bash
python main.py <file_path>
```

### Bulk Processing
```bash
python bulk_load_all.py
```

### Dependencies
```bash
pip install -r requirements.lock
```

## Development Guidelines

### Code Patterns
- Use base classes from `parsers/base.py` for new parsers
- Follow temporal consistency patterns with effective dates
- Implement comprehensive validation using `loader/validation.py`
- Use retry wrapper for database operations
- Log all operations with appropriate levels

### Database Conventions
- All item tables include denormalized `state_code` and `effective_date` fields
- Use JSONB for flexible additional data storage
- Maintain temporal consistency with exclusion constraints
- Follow naming conventions: `{document_type}_items` for main tables

### Error Handling
- Parser error threshold: 10% failure rate tolerance
- Use `retry_wrapper.py` for transient failures
- Track all operations in `loading_status` table
- Implement graceful degradation for document format variations

### Configuration
- State mappings: `config/state_names.json`
- Section mappings: `config/section_mapping.json`
- Database config should be in root `config.py`

## File Structure

```
├── main.py                    # Single file processor
├── bulk_load_all.py          # Bulk processor
├── config.py                 # Database configuration
├── config/                   # JSON configuration files
├── loader/                   # Database loading modules
├── parsers/                  # Document parsers
├── monitoring/               # Performance monitoring
├── sql/                      # Database schema and functions
└── tests/                    # Test suite
```

## Database Schema

The system uses PostgreSQL with temporal data management:
- Core tables: `states`, `document_types`, `document_versions`
- Item tables: `lod_items`, `cert_items`, `tap_items`
- Status tracking: `loading_status`
- Helper functions for temporal queries and change tracking

## Important Notes

- The project handles multiple document format versions automatically
- Temporal consistency is enforced at the database level
- All operations are logged and monitored
- The system supports both incremental and full reloads
- PDF parsing is available for TAP documents using pdfplumber