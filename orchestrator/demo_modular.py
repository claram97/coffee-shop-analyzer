#!/usr/bin/env python3
"""
Demo script showing the modular architecture in action.
This script demonstrates how to use the new modular components.
"""

import logging
import sys
import os

# Add the orchestrator directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

from common.protocol import Opcodes, NewMenuItems, BetsRecvSuccess
from common.network import MessageHandler, ResponseHandler
from common.processing import create_filtered_data_batch, BatchProcessor


def demo_protocol_usage():
    """Demonstrate protocol module usage."""
    print("=== Protocol Module Demo ===")
    
    # Show opcodes
    print(f"NEW_MENU_ITEMS opcode: {Opcodes.NEW_MENU_ITEMS}")
    print(f"FINISHED opcode: {Opcodes.FINISHED}")
    
    # Create a message instance
    menu_msg = NewMenuItems()
    print(f"Created NewMenuItems with opcode: {menu_msg.opcode}")
    print(f"Required keys: {menu_msg.required_keys}")
    print()


def demo_processing_usage():
    """Demonstrate processing module usage."""
    print("=== Processing Module Demo ===")
    
    # Create a batch processor
    processor = BatchProcessor()
    
    # Show mappings
    print("Available table mappings:")
    for opcode in [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_STORES, Opcodes.NEW_USERS]:
        table_name = processor.get_table_name_for_opcode(opcode)
        query_ids = processor.get_query_ids_for_opcode(opcode)
        print(f"  {table_name} (opcode {opcode}) -> queries {query_ids}")
    
    print()


def demo_network_usage():
    """Demonstrate network module usage."""
    print("=== Network Module Demo ===")
    
    # Create message handler
    handler = MessageHandler()
    
    # Show message type checking
    class MockMessage:
        def __init__(self, opcode):
            self.opcode = opcode
    
    data_msg = MockMessage(Opcodes.NEW_MENU_ITEMS)
    finished_msg = MockMessage(Opcodes.FINISHED)
    
    print(f"NEW_MENU_ITEMS is data message: {handler.is_data_message(data_msg)}")
    print(f"FINISHED is data message: {handler.is_data_message(finished_msg)}")
    
    # Show status text conversion
    print(f"Status 0: {handler.get_status_text(0)}")
    print(f"Status 1: {handler.get_status_text(1)}")
    print(f"Status 2: {handler.get_status_text(2)}")
    print()


def demo_architecture_comparison():
    """Show the difference between monolithic and modular approaches."""
    print("=== Architecture Comparison ===")
    
    print("Monolithic (original protocol.py):")
    print("  - 600+ lines in single file")
    print("  - Mixed concerns: parsing, entities, serialization, filtering")
    print("  - Hard to test individual components")
    print("  - Single point of failure")
    print()
    
    print("Modular (new architecture):")
    print("  - Separated into focused modules:")
    print("    * protocol/ - Message definitions and parsing")
    print("    * network/  - Connection and message handling")
    print("    * processing/ - Data filtering and batch processing")
    print("  - Each module has single responsibility")
    print("  - Easy to test and mock individual components")
    print("  - Better code organization and maintainability")
    print("  - Enables different implementations (e.g., async networking)")
    print()


def main():
    """Main demo function."""
    print("Coffee Shop Analyzer - Modular Architecture Demo")
    print("=" * 50)
    print()
    
    # Setup basic logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Run demos
    demo_protocol_usage()
    demo_processing_usage() 
    demo_network_usage()
    demo_architecture_comparison()
    
    print("Demo completed! The modular architecture is ready to use.")
    print()
    print("To use the modular orchestrator:")
    print("  1. Set USE_MODULAR_ARCHITECTURE=true in config.ini")
    print("  2. Run: python main_modular.py")
    print()
    print("To use the legacy orchestrator:")
    print("  1. Set USE_MODULAR_ARCHITECTURE=false in config.ini") 
    print("  2. Run: python main_modular.py")
    print("  3. Or use the original: python main.py")


if __name__ == "__main__":
    main()