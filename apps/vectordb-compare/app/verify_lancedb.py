"""
LanceDB Ingestion Verification Engine
"""

import os
from verify_base import VerificationEngine


class LanceDBVerificationEngine(VerificationEngine):
    """LanceDB ingestion verification engine."""

    def __init__(self):
        super().__init__("ldb")

    def verify_ingestion(self, collection_name, dims, expected_elements, **kwargs):
        """Verify LanceDB ingestion."""
        import utils as u

        # Use centralized connector from utils
        db = u.create_connector_lancedb()

        try:
            # Check if table exists
            if collection_name not in db.table_names():
                print(f"Error: Table {collection_name} not found")
                return self.log_result(collection_name, expected_elements, 0, False)

            # Open table and count rows
            table = db.open_table(collection_name)

            # Use count() method or search to get row count (LanceDB Cloud compatible)
            try:
                # Try to get count using count() method if available
                actual_count = table.count_rows()
            except AttributeError:
                # Fallback: use search to estimate count
                try:
                    # Search for all rows to get count
                    all_results = table.search().limit(expected_elements + 1000).to_list()
                    actual_count = len(all_results)
                except Exception as search_error:
                    # Final fallback: try a simple search to see if table has any data
                    try:
                        sample_results = table.search().limit(1).to_list()
                        if sample_results:
                            print(f"Warning: Cannot determine exact row count for {collection_name}. "
                                  f"Table exists and has data but verification method failed: {search_error}")
                            return self.log_result(collection_name, expected_elements, -1, False)
                        else:
                            # Empty table
                            actual_count = 0
                    except Exception as final_error:
                        print(
                            f"Error: Cannot access table data for {collection_name}: {final_error}")
                        return self.log_result(collection_name, expected_elements, -1, False)

            # Verify vector dimensions if possible
            if actual_count > 0:
                try:
                    # Get a sample row using search
                    sample_results = table.search().limit(1).to_list()
                    if sample_results and len(sample_results) > 0:
                        sample_row = sample_results[0]
                        if 'vector' in sample_row:
                            actual_dims = len(sample_row['vector'])
                            if actual_dims != dims:
                                print(
                                    f"Error: Dimension mismatch: expected {dims}, got {actual_dims}")
                                return self.log_result(collection_name, expected_elements, actual_count, False)
                except Exception as dim_error:
                    print(f"Warning: Could not verify dimensions: {dim_error}")
                    # Continue without dimension check

            success = actual_count == expected_elements
            return self.log_result(collection_name, expected_elements, actual_count, success)

        except Exception as e:
            print(f"Error: {str(e)}")
            return self.log_result(collection_name, expected_elements, 0, False)


def verify_lancedb(set_name, dims, expected_elements):
    """Entry point for LanceDB ingestion verification."""
    engine = LanceDBVerificationEngine()
    return engine.verify_ingestion(set_name, dims, expected_elements)
