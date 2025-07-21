# parsers/csv_parser.py - Base CSV parser for SST files
from __future__ import annotations
import csv
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging
import re

logger = logging.getLogger('sst.parser.csv')


class SSTCSVParser:
    """Base parser for SST CSV files with metadata headers."""
    
    def __init__(self, data_start_fallback: int = 4):
        self.metadata = {}
        self.data_start_fallback = data_start_fallback
        
    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding by checking for BOM and null bytes."""
        with open(file_path, 'rb') as f:
            raw = f.read(min(32, f.seek(0, 2)))
            f.seek(0)
            
        # Check for UTF-16 LE BOM or null bytes pattern
        if raw.startswith(b'\xff\xfe') or (b'\x00' in raw[:100]):
            return 'utf-16-le'
        elif raw.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
        else:
            return 'utf-8'
    
    def read_csv_with_encoding(self, file_path: Path) -> Tuple[Dict, List[List[str]]]:
        """Read CSV file with proper encoding detection."""
        try:
            encoding = self.detect_encoding(file_path)
            logger.info(f"Detected encoding: {encoding} for {file_path.name}")
            
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
                
            # Parse CSV content
            reader = csv.reader(io.StringIO(content))
            rows = list(reader)
            
            if not rows:
                logger.warning(f"No rows found in CSV file: {file_path.name}")
                return {}, []
            
            logger.debug(f"Read {len(rows)} rows from {file_path.name}")
            
            # Extract metadata from header rows
            metadata = self._extract_metadata(rows)
            
            # Find where actual data starts
            data_start_row = self._find_data_start(rows)
            
            data_rows = rows[data_start_row:]
            logger.info(f"Found {len(data_rows)} data rows starting from row {data_start_row + 1} in {file_path.name}")
            
            return metadata, data_rows
            
        except UnicodeDecodeError as e:
            logger.error(f"Encoding error reading {file_path.name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path.name}: {e}")
            raise
    
    def _extract_metadata(self, rows: List[List[str]]) -> Dict:
        """Extract metadata from header rows."""
        metadata = {}
        
        # Row 2 typically contains the main metadata
        if len(rows) > 1 and len(rows[1]) >= 9:
            header_row = rows[0]
            data_row = rows[1]
            
            # Map headers to values
            for i, header in enumerate(header_row[1:], 1):  # Skip first column
                if i < len(data_row) and header and data_row[i]:
                    metadata[header] = data_row[i]
        
        return metadata
    
    def _find_data_start(self, rows: List[List[str]]) -> int:
        """Find where the actual data starts (after metadata rows)."""
        # Look for rows that don't look like metadata
        for i, row in enumerate(rows):
            if i < 3:  # Skip first few rows
                continue
                
            # Check if this looks like a data row
            if len(row) >= 3 and not any(
                keyword in str(row[0:3]).lower() 
                for keyword in ['state', 'version', 'library', 'section', 'tax admin', 'certificate']
            ):
                return i
        
        # Log fallback usage
        logger.warning(f"Could not detect data start row, using fallback row {self.data_start_fallback}")
        return self.data_start_fallback


class LODCSVParser(SSTCSVParser):
    """Parser for Library of Definitions CSV files."""
    
    # Flexible header mappings for various column name variations
    HEADER_MAPPINGS = {
        'code': ['item number', 'item no', 'item', 'item #', 'code', 'number'],
        'description': ['description', 'desc', 'desc.', 'definition'],
        'treatment': ['treatment', 'taxability', 'status', 'tax status'],
        'reference': ['reference', 'statute', 'citation', 'ref', 'cite'],
        'comment': ['comment', 'comments', 'notes', 'note', 'remarks'],
        'taxable': ['taxable', 'tax', 't'],
        'exempt': ['exempt', 'exemption', 'e'],
        'included': ['included', 'include', 'inc'],
        'excluded': ['excluded', 'exclude', 'exc'],
        'threshold': ['threshold', 'limit', 'amount']
    }
    
    def parse(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Parse LOD CSV file and return categorized items."""
        metadata, data_rows = self.read_csv_with_encoding(file_path)
        self.metadata = metadata
        
        # Find the actual data columns
        items = {
            "admin_definitions": [],
            "product_definitions": [],
            "holiday_items": []
        }
        
        current_section = None
        column_mapping = {}
        
        for row in data_rows:
            if not row or not any(row):  # Skip empty rows
                continue
                
            # Detect section headers
            first_cell = str(row[0]).strip()
            
            if 'administrative definition' in first_cell.lower():
                current_section = 'admin'
                column_mapping = self._parse_column_headers(row)
                continue
            elif 'product definition' in first_cell.lower() or 'product/service definition' in first_cell.lower():
                current_section = 'product'
                column_mapping = self._parse_column_headers(row)
                continue
            elif 'sales tax holiday' in first_cell.lower():
                current_section = 'holiday'
                column_mapping = self._parse_column_headers(row)
                continue
            
            # Parse data rows based on current section
            if current_section and column_mapping and self._is_data_row(row):
                item = self._parse_data_row(row, column_mapping, current_section, len(data_rows) - data_rows.index(row))
                if item:
                    if current_section == 'admin':
                        items['admin_definitions'].append(item)
                    elif current_section == 'product':
                        items['product_definitions'].append(item)
                    elif current_section == 'holiday':
                        items['holiday_items'].append(item)
        
        # Log summary of parsed items
        total_items = sum(len(items[key]) for key in items)
        logger.info(f"Parsed LOD items: {len(items['admin_definitions'])} admin, "
                   f"{len(items['product_definitions'])} product, "
                   f"{len(items['holiday_items'])} holiday (total: {total_items})")
        
        return items
    
    def _parse_column_headers(self, row: List[str]) -> Dict[str, int]:
        """Parse column headers with flexible matching and return mapping."""
        mapping = {}
        
        for i, cell in enumerate(row):
            cell_clean = str(cell).strip().lower()
            if not cell_clean:
                continue
                
            # Try to match this cell to a known field using flexible matching
            for field_name, variations in self.HEADER_MAPPINGS.items():
                for variation in variations:
                    # Use fuzzy matching - check if variation is contained in cell or vice versa
                    if (variation in cell_clean or 
                        cell_clean in variation or 
                        self._fuzzy_match(cell_clean, variation)):
                        mapping[field_name] = i
                        logger.debug(f"Mapped column '{cell}' to field '{field_name}' at index {i}")
                        break
                if field_name in mapping:  # Break outer loop if found
                    break
        
        if not mapping:
            logger.warning(f"No column mappings found for header row: {row}")
        
        return mapping
    
    def _fuzzy_match(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Simple fuzzy matching for similar header names."""
        # Remove common punctuation and spaces
        clean1 = re.sub(r'[^a-zA-Z0-9]', '', text1).lower()
        clean2 = re.sub(r'[^a-zA-Z0-9]', '', text2).lower()
        
        if not clean1 or not clean2:
            return False
            
        # Simple character overlap ratio
        overlap = len(set(clean1) & set(clean2))
        min_len = min(len(clean1), len(clean2))
        
        return overlap / min_len >= threshold if min_len > 0 else False
    
    def _is_data_row(self, row: List[str]) -> bool:
        """Check if this is a data row (not a header or section divider)."""
        if not row or len(row) < 2:
            return False
            
        first_cell = str(row[0]).strip()
        
        # Skip empty rows
        if not first_cell:
            return False
            
        # Check for section headers (common patterns)
        section_patterns = [
            r'^(administrative|product|sales tax holiday)',
            r'^(section|part|group)\s+\d+',
            r'^(library|definition|certificate)',
            r'^(taxable|exempt|included|excluded)$'
        ]
        
        first_cell_lower = first_cell.lower()
        for pattern in section_patterns:
            if re.match(pattern, first_cell_lower):
                logger.debug(f"Skipping section header: {first_cell}")
                return False
        
        # Item codes are typically numeric or alphanumeric with reasonable length
        if first_cell and (first_cell.isdigit() or 
                          (len(first_cell) <= 15 and any(c.isdigit() for c in first_cell))):
            return True
            
        return False
    
    def _parse_data_row(self, row: List[str], mapping: Dict[str, int], section: str, row_number: int = 0) -> Optional[Dict]:
        """Parse a data row into an item dictionary."""
        if not mapping:
            logger.warning(f"No column mapping available for row {row_number}: {row}")
            return None
            
        item = {
            'item_type': f'{section}_definition' if section != 'holiday' else 'holiday_item'
        }
        
        # Extract values based on mapping
        for field, index in mapping.items():
            if index < len(row):
                value = row[index].strip()
                
                # Handle treatment column for taxable/exempt
                if field == 'treatment':
                    treatment_lower = value.lower()
                    if 'taxable' in treatment_lower or treatment_lower == 't':
                        item['taxable'] = True
                        item['exempt'] = False
                    elif 'exempt' in treatment_lower or treatment_lower == 'e':
                        item['taxable'] = False
                        item['exempt'] = True
                    elif 'included' in treatment_lower:
                        item['included'] = True
                        item['excluded'] = False
                    elif 'excluded' in treatment_lower:
                        item['included'] = False
                        item['excluded'] = True
                else:
                    item[field] = value
            else:
                logger.debug(f"Row {row_number}: Column index {index} for field '{field}' exceeds row length {len(row)}")
        
        # Ensure required fields
        if 'code' not in item or not item.get('code'):
            # Try to get code from first column
            if row and row[0].strip():
                item['code'] = row[0].strip()
                logger.debug(f"Row {row_number}: Using first column as code: {item['code']}")
            else:
                logger.warning(f"Row {row_number}: No code found, skipping row: {row}")
                return None
                
        return item


class TAPCSVParser(SSTCSVParser):
    """Parser for Tax Administration Practices CSV files."""
    
    def parse(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Parse TAP CSV file and return items."""
        metadata, data_rows = self.read_csv_with_encoding(file_path)
        self.metadata = metadata
        
        items = []
        current_group = ""
        question_number = 1
        
        for row_idx, row in enumerate(data_rows, start=1):
            try:
                if not row or not any(row):
                    continue
                    
                # TAP files have a simpler structure
                # Usually: Practice number, Description, Answer/Treatment, Citation, Comments
                
                if len(row) >= 2:
                    first_cell = str(row[0]).strip()
                    
                    # Check if this is a group header
                    if 'disclosed practice' in first_cell.lower() or 'group' in first_cell.lower():
                        current_group = first_cell
                        continue
                    
                    # Parse as data row
                    item = {
                        'item_type': 'tap_practice',
                        'question_number': str(question_number),
                        'group_name': current_group,
                        'question_text': row[1].strip() if len(row) > 1 else '',
                        'answer': row[2].strip() if len(row) > 2 else '',
                        'citation': row[3].strip() if len(row) > 3 else '',
                        'notes': row[4].strip() if len(row) > 4 else ''
                    }
                    
                    if item['question_text']:  # Only add if there's actual content
                        items.append(item)
                        question_number += 1
            except Exception as e:
                logger.error(f"Error parsing TAP row {row_idx}: {e}. Row data: {row}")
                continue
        
        logger.info(f"Parsed {len(items)} TAP practice items from CSV")
        return {'tap_practices': items}


class CertCSVParser(SSTCSVParser):
    """Parser for Certificate of Compliance CSV files."""
    
    def parse(self, file_path: Path) -> Dict[str, List[Dict]]:
        """Parse Certificate CSV file and return items."""
        metadata, data_rows = self.read_csv_with_encoding(file_path)
        self.metadata = metadata
        
        items = []
        current_section = ""
        
        for row_idx, row in enumerate(data_rows, start=1):
            try:
                if not row or not any(row):
                    continue
                    
                # Certificate files structure:
                # Section, Topic/Question, Description, Compliance (Y/N), Citation, Dates, Notes
                
                if len(row) >= 4:
                    section = str(row[0]).strip()
                    
                    # Update section if this looks like a section header
                    if section and 'section' in section.lower() and len(row[1].strip()) > 20:
                        current_section = section
                        continue
                    
                    # Parse data row
                    item = {
                        'item_type': 'cert_item',
                        'section': current_section or section,
                        'question_number': section,  # Use section as question number
                        'topic': row[1].strip() if len(row) > 1 else '',
                        'question_text': row[2].strip() if len(row) > 2 else '',
                        'answer': row[3].strip() if len(row) > 3 else '',
                        'citation': row[4].strip() if len(row) > 4 else '',
                        'effective_dates': row[5].strip() if len(row) > 5 else '',
                        'notes': row[6].strip() if len(row) > 6 else ''
                    }
                    
                    # Normalize answer to compliance_met
                    answer_upper = item['answer'].upper()
                    if answer_upper in ['Y', 'YES']:
                        item['compliance_met'] = True
                    elif answer_upper in ['N', 'NO']:
                        item['compliance_met'] = False
                    else:
                        item['compliance_met'] = None
                    
                    if item['question_text'] or item['topic']:  # Add if there's content
                        items.append(item)
            except Exception as e:
                logger.error(f"Error parsing Certificate row {row_idx}: {e}. Row data: {row}")
                continue
        
        logger.info(f"Parsed {len(items)} certificate items from CSV")
        return {'cert_items': items}