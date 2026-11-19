#!/usr/bin/env python3
"""
Test script for Meta 02 Configuration and Parquet Generation
This script tests the configuration manager and prepares for Linux server testing
"""

import sys
import os
from pathlib import Path
import logging
import json
from datetime import datetime

# Add project root directory to path for imports
current_dir = Path(__file__).parent
project_root = current_dir.parent  # Go up one level from tests/ to project root
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "utils"))  # Add utils folder to path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_configuration_manager():
    """Test the configuration manager."""
    try:
        # Import from utils directory
        from utils.meta_config_manager import MetaConfigManager
        
        print("\n" + "="*60)
        print("🧪 TESTING META CONFIGURATION MANAGER")
        print("="*60)
        
        # Initialize configuration manager
        config_manager = MetaConfigManager()
        
        # Test basic functionality
        print(f"\n📁 Configuration directory: {config_manager.config_dir}")
        
        # Test Meta 02 configuration
        print(f"\n📋 Testing Meta 02 Configuration:")
        
        # Test date cutoffs
        cutoff_date = config_manager.get_date_cutoff("meta_02", "meta02_cutoff_date")
        print(f"  📅 Meta 02 cutoff date: {cutoff_date}")
        
        long_term_cutoff = config_manager.get_date_cutoff("meta_02", "meta02_15year_cutoff")
        print(f"  📅 Meta 02 15-year cutoff: {long_term_cutoff}")
        
        # Test situacao codes
        dism2_codes = config_manager.get_situacao_codes("meta_02", "dism2")
        print(f"  🔢 DISM2 situacao codes: {dism2_codes}")
        
        exclusion_codes = config_manager.get_exclusion_codes("meta_02", "dism2")
        print(f"  🚫 DISM2 exclusion codes: {exclusion_codes}")
        
        # Test paths
        base_path = config_manager.get_system_path("situacao_base_path")
        print(f"  📂 Situacao base path: {base_path}")
        
        export_path = config_manager.get_system_path("meta_02")
        print(f"  📤 Meta 02 export path: {export_path}")
        
        # Test available situacoes
        available = config_manager.get_available_situacoes("ejud")
        print(f"  🔧 Available EJUD situacoes: {len(available)} codes")
        print(f"      First 10: {available[:10]}")
        
        print(f"\n✅ Configuration manager test completed successfully!")
        return True
    
    except Exception as e:
        print(f"\n❌ Configuration manager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_json_configuration_files():
    """Validate all JSON configuration files are present and properly formatted."""
    try:
        print("\n" + "="*60)
        print("📋 VALIDATING JSON CONFIGURATION FILES")
        print("="*60)
        
        config_dir = project_root / "config"  # JSON files are in root config folder
        
        # Required configuration files
        required_configs = {
            "meta_config.json": "Meta-specific rules and situacao codes",
            "date_rules_config.json": "Date calculation rules",
            "system_config.json": "System paths and processing configuration",
            "variable_definitions.json": "Variable metadata definitions"
        }
        
        validation_results = []
        all_valid = True
        
        for config_file, description in required_configs.items():
            config_path = config_dir / config_file
            print(f"\n📄 Validating {config_file}")
            print(f"   Description: {description}")
            
            # Check if file exists
            if not config_path.exists():
                print(f"   ❌ File not found: {config_path}")
                validation_results.append(f"Missing: {config_file}")
                all_valid = False
                continue
            
            # Check if file is valid JSON
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    print(f"   ✅ Valid JSON format")
                    
                    # Specific validations for each config file
                    if config_file == "meta_config.json":
                        validation_results.append(validate_meta_config(config_data, config_file))
                    elif config_file == "date_rules_config.json":
                        validation_results.append(validate_date_rules_config(config_data, config_file))
                    elif config_file == "system_config.json":
                        validation_results.append(validate_system_config(config_data, config_file))
                    elif config_file == "variable_definitions.json":
                        validation_results.append(validate_variable_definitions(config_data, config_file))
                        
            except json.JSONDecodeError as e:
                print(f"   ❌ Invalid JSON format: {e}")
                validation_results.append(f"Invalid JSON: {config_file}")
                all_valid = False
            except Exception as e:
                print(f"   ⚠️  Validation warning: {e}")
                validation_results.append(f"Warning: {config_file}")
        
        # Summary
        print(f"\n📊 JSON Configuration Validation Summary:")
        for result in validation_results:
            if result and result.startswith(('Missing:', 'Invalid JSON:', 'Error:')):
                print(f"   ❌ {result}")
                all_valid = False
            elif result and result.startswith('Warning:'):
                print(f"   ⚠️  {result}")
            elif result:
                print(f"   ✅ {result}")
        
        if all_valid:
            print(f"\n✅ All JSON configuration files are valid!")
        else:
            print(f"\n❌ Some JSON configuration files have issues!")
        
        return all_valid
        
    except Exception as e:
        print(f"\n❌ JSON configuration validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_meta_config(config_data, filename):
    """Validate meta_config.json structure."""
    try:
        # Check required top-level keys
        required_keys = ["meta_02"]
        for key in required_keys:
            if key not in config_data:
                return f"Error: {filename} missing required key: {key}"
        
        # Check Meta 02 structure
        meta_02 = config_data["meta_02"]
        
        # Check date_cutoffs
        if "date_cutoffs" in meta_02:
            date_cutoffs = meta_02["date_cutoffs"]
            for date_key, date_value in date_cutoffs.items():
                try:
                    datetime.strptime(date_value, "%Y-%m-%d")
                except ValueError:
                    return f"Error: {filename} invalid date format in {date_key}: {date_value}"
        
        # Check situacao_codes
        if "situacao_codes" in meta_02:
            situacao_codes = meta_02["situacao_codes"]
            for var_name, codes in situacao_codes.items():
                if not isinstance(codes, list) or not all(isinstance(x, int) for x in codes):
                    return f"Error: {filename} invalid situacao codes for {var_name}"
        
        print(f"     ✅ Meta config structure is valid")
        return f"Valid: {filename}"
        
    except Exception as e:
        return f"Error: {filename} validation failed: {e}"

def validate_date_rules_config(config_data, filename):
    """Validate date_rules_config.json structure."""
    try:
        # Check for date calculation rules
        if "date_calculation_rules" not in config_data:
            return f"Error: {filename} missing date_calculation_rules"
        
        print(f"     ✅ Date rules structure is valid")
        return f"Valid: {filename}"
        
    except Exception as e:
        return f"Error: {filename} validation failed: {e}"

def validate_system_config(config_data, filename):
    """Validate system_config.json structure."""
    try:
        # Check required sections
        required_sections = ["paths", "processing"]
        for section in required_sections:
            if section not in config_data:
                return f"Error: {filename} missing required section: {section}"
        
        # Check paths structure
        paths = config_data["paths"]
        if "export_base_path" not in paths:
            return f"Error: {filename} missing export_base_path in paths"
        
        print(f"     ✅ System config structure is valid")
        return f"Valid: {filename}"
        
    except Exception as e:
        return f"Error: {filename} validation failed: {e}"

def validate_variable_definitions(config_data, filename):
    """Validate variable_definitions.json structure."""
    try:
        # Check for variables section
        if "variables" not in config_data:
            return f"Error: {filename} missing variables section"
        
        variables = config_data["variables"]
        
        # Check each variable has required fields
        for var_name, var_def in variables.items():
            required_fields = ["description", "calculation_method"]
            for field in required_fields:
                if field not in var_def:
                    return f"Error: {filename} variable {var_name} missing field: {field}"
        
        print(f"     ✅ Variable definitions structure is valid")
        return f"Valid: {filename}"
        
    except Exception as e:
        return f"Error: {filename} validation failed: {e}"

def check_meta02_files():
    """Check if Meta 02 files are ready for testing."""
    print("\n" + "="*60)
    print("📋 CHECKING META 02 FILES")
    print("="*60)
      # Define file locations correctly
    required_files = {
        "meta_02_ejud.py": project_root,  # Main file in project root
        "utils/meta_results_exporter.py": project_root, 
        "utils/meta_logging_system.py": project_root,
        "utils/meta_scheduler.py": project_root,
        "utils/hybrid_parquet_loader.py": project_root,
        "utils/spark_meta_utils.py": project_root,
        "utils/meta_config_manager.py": project_root,
        "config/meta_config.json": project_root,
        "config/system_config.json": project_root,
        "config/date_rules_config.json": project_root,
        "config/variable_definitions.json": project_root
    }
    
    missing_files = []
    for file_name, base_path in required_files.items():
        file_path = base_path / file_name
        if file_path.exists():
            print(f"  ✅ {file_name}")
        else:
            print(f"  ❌ {file_name} - MISSING")
            missing_files.append(file_name)
    
    if missing_files:
        print(f"\n⚠️  Missing files: {missing_files}")
        return False
    else:
        print(f"\n✅ All required files present!")
        return True

def check_import_syntax():
    """Check if the Python files have correct syntax."""
    print("\n" + "="*60)
    print("🐍 CHECKING PYTHON SYNTAX")
    print("="*60)
    
    # Define files to check with their correct paths
    files_to_check = {
        "meta_02_ejud.py": project_root,
        "utils/meta_config_manager.py": project_root,
        "utils/meta_results_exporter.py": project_root,
        "utils/meta_logging_system.py": project_root, 
        "utils/meta_scheduler.py": project_root,
        "utils/hybrid_parquet_loader.py": project_root,
        "utils/spark_meta_utils.py": project_root
    }
    
    syntax_errors = []
    for file_name, base_path in files_to_check.items():
        file_path = base_path / file_name
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                compile(content, file_name, 'exec')
                print(f"  ✅ {file_name} - Syntax OK")
            except SyntaxError as e:
                print(f"  ❌ {file_name} - Syntax Error: {e}")
                syntax_errors.append((file_name, e))
            except Exception as e:
                print(f"  ⚠️  {file_name} - Check Warning: {e}")
        else:
            print(f"  ⚠️  {file_name} - File not found")
    
    if syntax_errors:
        print(f"\n⚠️  Syntax errors found: {len(syntax_errors)}")
        for file_name, error in syntax_errors:
            print(f"    ❌ {file_name}: {error}")
        return False
    else:
        print(f"\n✅ All files have correct syntax!")
        return True

def generate_server_test_commands():
    """Generate commands to test on Linux server."""
    print("\n" + "="*60)
    print("🖥️  LINUX SERVER TEST COMMANDS")
    print("="*60)
    
    commands = [
        "# Navigate to project directory",
        "cd /path/to/meta_cnj2026",
        "",
        "# Test configuration manager",
        "python3 meta_config_manager.py",
        "",
        "# Test Meta 02 with small date range (testing)",  
        "python3 meta_02_ejud.py --start-date 2024-12-01 --end-date 2024-12-01",
        "",
        "# Check export results",
        "ls -la /opt/parquet/data/prata/ejud_meta2025_meta02/",
        "",
        "# View logs",
        "tail -f logs/meta_02_*.log",
        "",
        "# Test full month (production)",
        "python3 meta_02_ejud.py --start-date 2024-12-01 --end-date 2024-12-31"
    ]
    
    for cmd in commands:
        print(f"  {cmd}")
    
    print(f"\n📋 Save these commands for server testing!")

def main():
    """Main test function."""
    print("🚀 PREPARING META 02 FOR LINUX SERVER TESTING")
    
    # Run all tests
    tests_passed = []
    
    # Test 1: Configuration Manager
    tests_passed.append(test_configuration_manager())
    
    # Test 2: File Availability  
    tests_passed.append(check_meta02_files())
    
    # Test 3: Syntax Check
    tests_passed.append(check_import_syntax())
    
    # Generate server commands
    generate_server_test_commands()
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    total_tests = len(tests_passed)
    passed_tests = sum(tests_passed)
    
    if passed_tests == total_tests:
        print(f"✅ ALL TESTS PASSED ({passed_tests}/{total_tests})")
        print("🚀 Ready for Linux server testing!")
    else:
        print(f"⚠️  SOME TESTS FAILED ({passed_tests}/{total_tests})")
        print("🔧 Please fix issues before server testing")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
