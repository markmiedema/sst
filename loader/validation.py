# loader/validation.py
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import re
from dataclasses import dataclass
import logging

@dataclass
class ValidationResult:
    """Container for validation results."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    
    def add_error(self, error: str):
        self.errors.append(error)
        self.is_valid = False
        
    def add_warning(self, warning: str):
        self.warnings.append(warning)


class SchemaValidator:
    """Validates data structure and types."""
    
    def __init__(self):
        self.logger = logging.getLogger('sst.validation.schema')
        
    def validate_lod_item(self, item: Dict) -> ValidationResult:
        """Validate a Library of Definitions item."""
        result = ValidationResult(True, [], [])
        
        # Required fields
        required = ['item_type', 'code', 'description']
        for field in required:
            if not item.get(field):
                result.add_error(f"Missing required field: {field}")
        
        # Item type validation
        valid_types = {'admin_definition', 'product_definition', 'holiday_item'}
        if item.get('item_type') not in valid_types:
            result.add_error(f"Invalid item_type: {item.get('item_type')}")
        
        # Code format validation
        code = item.get('code', '')
        if code and not self._validate_code_format(code):
            result.add_warning(f"Unusual code format: {code}")
        
        # Boolean field consistency
        if item.get('taxable') and item.get('exempt'):
            result.add_error("Item cannot be both taxable and exempt")
        
        # Threshold validation for holiday items
        if item.get('item_type') == 'holiday_item':
            threshold = item.get('threshold')
            if threshold is not None:
                try:
                    float(threshold)
                except (ValueError, TypeError):
                    result.add_error(f"Invalid threshold value: {threshold}")
        
        return result
    
    def validate_cert_item(self, item: Dict) -> ValidationResult:
        """Validate a Certificate of Compliance item."""
        result = ValidationResult(True, [], [])
        
        # Required fields
        if not item.get('question_number'):
            result.add_error("Missing question number")
        if not item.get('question_text'):
            result.add_error("Missing question text")
        
        # Answer validation
        answer = item.get('answer', '').strip().upper()
        if answer and answer not in ['YES', 'NO', 'N/A', 'NA']:
            result.add_warning(f"Non-standard answer: {answer}")
        
        return result
    
    def validate_tap_item(self, item: Dict) -> ValidationResult:
        """Validate a Tax Administration Practice item."""
        result = ValidationResult(True, [], [])
        
        # Similar to cert validation
        if not item.get('question_number'):
            result.add_error("Missing question number")
        if not item.get('question_text'):
            result.add_error("Missing question text")
        
        # Group name validation
        if not item.get('group_name'):
            result.add_warning("Missing group name")
        
        return result
    
    def _validate_code_format(self, code: str) -> bool:
        """Validate code format."""
        # Codes should be alphanumeric with possible dots/dashes
        pattern = r'^[A-Z0-9][A-Z0-9.\-]*$'
        return bool(re.match(pattern, code.upper()))


class TemporalConsistencyValidator:
    """Validates temporal consistency across versions."""
    
    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger('sst.validation.temporal')
        
    def validate_version_sequence(self, state_code: str, doc_type: str) -> ValidationResult:
        """Check that versions are in proper sequence."""
        result = ValidationResult(True, [], [])
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT version, effective_date, valid_to
                FROM document_versions dv
                JOIN document_types dt ON dv.document_type_id = dt.document_type_id
                WHERE state_code = %s AND document_type = %s
                ORDER BY effective_date
            """, (state_code, doc_type))
            
            versions = cur.fetchall()
            
        # Check for gaps or overlaps
        for i in range(len(versions) - 1):
            curr_version, curr_eff, curr_valid_to = versions[i]
            next_version, next_eff, next_valid_to = versions[i + 1]
            
            # Check for gaps
            if curr_valid_to and curr_valid_to < next_eff:
                gap_days = (next_eff - curr_valid_to).days
                result.add_warning(
                    f"Gap of {gap_days} days between {curr_version} and {next_version}"
                )
            
            # Check version numbering
            if not self._is_version_sequential(curr_version, next_version):
                result.add_warning(
                    f"Non-sequential versions: {curr_version} -> {next_version}"
                )
        
        return result
    
    def validate_no_orphaned_items(self, state_code: str) -> ValidationResult:
        """Check for items without valid document versions."""
        result = ValidationResult(True, [], [])
        
        tables = ['lod_items', 'cert_items', 'tap_items']
        
        for table in tables:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {table} t
                    LEFT JOIN document_versions dv 
                        ON t.document_version_id = dv.document_version_id
                    WHERE dv.document_version_id IS NULL
                    AND t.state_code = %s
                """, (state_code,))
                
                orphan_count = cur.fetchone()[0]
                if orphan_count > 0:
                    result.add_error(
                        f"Found {orphan_count} orphaned items in {table}"
                    )
        
        return result
    
    def _is_version_sequential(self, v1: str, v2: str) -> bool:
        """Check if version numbers are sequential."""
        # Extract version numbers (e.g., v2024.0 -> 2024.0)
        pattern = r'v?(\d{4})\.(\d+)'
        
        match1 = re.match(pattern, v1)
        match2 = re.match(pattern, v2)
        
        if not match1 or not match2:
            return True  # Can't validate, assume OK
        
        year1, minor1 = int(match1.group(1)), int(match1.group(2))
        year2, minor2 = int(match2.group(1)), int(match2.group(2))
        
        # Sequential if same year with incremented minor, or next year
        return (year1 == year2 and minor2 == minor1 + 1) or \
               (year2 == year1 + 1 and minor2 == 0)


class DataQualityValidator:
    """Validates data quality and completeness."""
    
    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger('sst.validation.quality')
        
    def validate_state_coverage(self, doc_type: str, 
                               expected_states: List[str]) -> ValidationResult:
        """Check that all expected states have current documents."""
        result = ValidationResult(True, [], [])
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT state_code
                FROM current_document_versions cdv
                JOIN document_types dt ON cdv.document_type_id = dt.document_type_id
                WHERE document_type = %s
            """, (doc_type,))
            
            loaded_states = {row[0] for row in cur.fetchall()}
        
        missing = set(expected_states) - loaded_states
        if missing:
            result.add_error(
                f"Missing {doc_type} documents for states: {', '.join(sorted(missing))}"
            )
        
        return result
    
    def validate_data_freshness(self, max_age_days: int = 365) -> ValidationResult:
        """Check that documents aren't too old."""
        result = ValidationResult(True, [], [])
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT state_code, document_type, 
                       MAX(effective_date) as latest_date
                FROM current_document_versions cdv
                JOIN document_types dt ON cdv.document_type_id = dt.document_type_id
                GROUP BY state_code, document_type
                HAVING MAX(effective_date) < CURRENT_DATE - INTERVAL '%s days'
            """, (max_age_days,))
            
            stale_docs = cur.fetchall()
            
        for state, doc_type, latest_date in stale_docs:
            age_days = (date.today() - latest_date).days
            result.add_warning(
                f"{state} {doc_type} is {age_days} days old (last update: {latest_date})"
            )
        
        return result


# Validation orchestrator
class ValidationOrchestrator:
    """Runs all validations and generates reports."""
    
    def __init__(self, conn):
        self.conn = conn
        self.schema_validator = SchemaValidator()
        self.temporal_validator = TemporalConsistencyValidator(conn)
        self.quality_validator = DataQualityValidator(conn)
        self.logger = logging.getLogger('sst.validation')
        
    def validate_all(self, state_code: Optional[str] = None) -> Dict:
        """Run all validations and return comprehensive report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'state_filter': state_code,
            'results': {},
            'summary': {'errors': 0, 'warnings': 0}
        }
        
        # Get states to validate
        states = [state_code] if state_code else self._get_all_states()
        
        for state in states:
            state_results = {}
            
            # Temporal consistency
            for doc_type in ['LOD', 'TAP', 'CERT']:
                result = self.temporal_validator.validate_version_sequence(
                    state, doc_type
                )
                state_results[f'{doc_type}_sequence'] = {
                    'valid': result.is_valid,
                    'errors': result.errors,
                    'warnings': result.warnings
                }
                report['summary']['errors'] += len(result.errors)
                report['summary']['warnings'] += len(result.warnings)
            
            # Orphaned items
            result = self.temporal_validator.validate_no_orphaned_items(state)
            state_results['orphaned_items'] = {
                'valid': result.is_valid,
                'errors': result.errors,
                'warnings': result.warnings
            }
            report['summary']['errors'] += len(result.errors)
            report['summary']['warnings'] += len(result.warnings)
            
            report['results'][state] = state_results
        
        # Global quality checks
        freshness_result = self.quality_validator.validate_data_freshness()
        report['results']['data_freshness'] = {
            'valid': freshness_result.is_valid,
            'errors': freshness_result.errors,
            'warnings': freshness_result.warnings
        }
        report['summary']['warnings'] += len(freshness_result.warnings)
        
        return report
    
    def _get_all_states(self) -> List[str]:
        """Get all states in the database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT state_code FROM document_versions")
            return [row[0] for row in cur.fetchall()]