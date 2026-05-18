"""Benchmark file parser and executor."""
import os
import sys
import time

# Fix import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.logger import get_logger
from src.common.config import TOTAL_TICKETS

logger = get_logger(__name__)

class BenchmarkParser:
    """Parse and execute benchmark files."""
    
    def __init__(self, filepath):
        """
        Initialize parser.
        
        Args:
            filepath: Path to benchmark file
        """
        self.filepath = filepath
        self.operations = []
        self.operation_type = None  # 'unnumbered' or 'numbered'

    def parse(self):
        """
        Parse benchmark file.
        
        Format:
        - Unnumbered: BUY <client_id> <request_id>
        - Numbered: BUY <client_id> <seat_id> <request_id>
        
        Returns:
            List of operation dicts
        """
        if not os.path.exists(self.filepath):
            logger.error(f"Benchmark file not found: {self.filepath}")
            return []
        
        with open(self.filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split()
                if not parts or parts[0] != 'BUY':
                    logger.warning(f"Line {line_num}: Invalid operation: {line}")
                    continue
                
                try:
                    if len(parts) == 3:
                        # Unnumbered: BUY <client_id> <request_id>
                        op = {
                            "type": "unnumbered",
                            "client_id": parts[1],
                            "request_id": int(parts[2])
                        }
                        self.operation_type = "unnumbered"
                    elif len(parts) == 4:
                        # Numbered: BUY <client_id> <seat_id> <request_id>
                        op = {
                            "type": "numbered",
                            "client_id": parts[1],
                            "seat_id": int(parts[2]),
                            "request_id": int(parts[3])
                        }
                        self.operation_type = "numbered"
                    else:
                        logger.warning(f"Line {line_num}: Invalid format: {line}")
                        continue
                    
                    self.operations.append(op)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Line {line_num}: Parse error: {e}")
                    continue
        
        logger.info(f"Parsed {len(self.operations)} operations from {self.filepath}")
        if self.operation_type:
            logger.info(f"Operation type: {self.operation_type}")
        
        return self.operations

    def get_operations(self):
        """Get parsed operations."""
        return self.operations

    def validate_correctness(self, redis_backend):
        """
        Validate benchmark correctness.
        
        Returns:
            (is_valid, error_message)
        """
        if self.operation_type == "unnumbered":
            return self._validate_unnumbered(redis_backend)
        elif self.operation_type == "numbered":
            return self._validate_numbered(redis_backend)
        return False, "Unknown operation type"

    def _validate_unnumbered(self, redis_backend):
        """Validate unnumbered tickets correctness."""
        sold_count = redis_backend.get_unnumbered_count()
        expected = min(len(self.operations), TOTAL_TICKETS)
        
        if sold_count != expected:
            msg = f"Expected {expected} sold, got {sold_count}"
            logger.error(msg)
            return False, msg
        
        logger.info(f"✓ Unnumbered validation passed: {sold_count} tickets sold")
        return True, ""

    def _validate_numbered(self, redis_backend):
        """Validate numbered tickets correctness."""
        sold_seats = redis_backend.get_sold_seats()
        sold_count = len(sold_seats)
        
        # Check for duplicates (shouldn't happen if system works)
        if sold_count != len(set(sold_seats)):
            msg = "Duplicate seat sales detected!"
            logger.error(msg)
            return False, msg
        
        # Check all sold seats are valid
        invalid_seats = [s for s in sold_seats if not (1 <= int(s) <= TOTAL_TICKETS)]
        if invalid_seats:
            msg = f"Invalid seats found: {invalid_seats}"
            logger.error(msg)
            return False, msg
        
        logger.info(f"✓ Numbered validation passed: {sold_count} unique seats sold")
        return True, ""

def create_hotspot_benchmark(output_file, num_operations=20000, hotspot_percent=5, hotspot_traffic=80):
    """
    Create a benchmark file with hotspot contention.
    
    80% of requests target 5% of seats.
    
    Args:
        output_file: Output file path
        num_operations: Total number of operations
        hotspot_percent: Percentage of seats that are hotspots
        hotspot_traffic: Percentage of traffic targeting hotspots
    """
    import random
    
    total_seats = TOTAL_TICKETS
    hotspot_seats = max(1, int(total_seats * hotspot_percent / 100))
    hotspot_request_count = int(num_operations * hotspot_traffic / 100)
    normal_request_count = num_operations - hotspot_request_count
    
    operations = []
    
    # Generate hotspot requests
    hotspot_seat_ids = random.sample(range(1, total_seats + 1), hotspot_seats)
    for i in range(hotspot_request_count):
        seat_id = random.choice(hotspot_seat_ids)
        client_id = i % 100  # Cycle through 100 clients
        request_id = i
        operations.append(f"BUY {client_id} {seat_id} {request_id}")
    
    # Generate normal requests
    normal_seat_ids = [s for s in range(1, total_seats + 1) if s not in hotspot_seat_ids]
    for i in range(normal_request_count):
        seat_id = random.choice(normal_seat_ids)
        client_id = (i + hotspot_request_count) % 100
        request_id = i + hotspot_request_count
        operations.append(f"BUY {client_id} {seat_id} {request_id}")
    
    # Shuffle to mix hotspot and normal requests
    random.shuffle(operations)
    
    with open(output_file, 'w') as f:
        for op in operations:
            f.write(op + '\n')
    
    logger.info(f"Created hotspot benchmark: {output_file}")
    logger.info(f"  Total operations: {num_operations}")
    logger.info(f"  Hotspot seats: {hotspot_seats} ({hotspot_percent}%)")
    logger.info(f"  Hotspot traffic: {hotspot_request_count} ({hotspot_traffic}%)")
    logger.info(f"  Normal traffic: {normal_request_count} ({100-hotspot_traffic}%)")
