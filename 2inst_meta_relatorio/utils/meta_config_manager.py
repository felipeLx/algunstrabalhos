#!/usr/bin/env python3
"""
Configuration Manager for Meta CNJ 2026 Calculations
Loads and manages configuration from JSON files to eliminate hardcoded values
"""

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import logging

# Import parquet utilities for environment-aware path resolution
from .parquet_utils import resolve_path_config, get_environment_info

logger = logging.getLogger(__name__)

class MetaConfigManager:
    """
    Centralized configuration manager for Meta CNJ 2026 calculations.
    Loads configuration from JSON files and provides easy access to:
    - Meta-specific rules and situacao codes
    - Date calculation rules  
    - System paths and processing configuration
    - Variable definitions and metadata
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """Initialize configuration manager."""
        if config_dir is None:
            # Get project root directory (go up one level from utils/)
            base_path = Path(__file__).parent.parent  # Go up from utils/ to project root
            
            # Priority order: json_files, json_file, config (all in project root)
            possible_dirs = [
                base_path / "json_files",
                base_path / "json_file", 
                base_path / "config"
            ]
            
            # Find first directory that exists and has config files
            self.config_dir = None
            for dir_path in possible_dirs:
                if dir_path.exists():
                    # Check if it has at least one config file
                    config_files = list(dir_path.glob("*.json"))
                    if config_files:
                        self.config_dir = dir_path
                        logger.info(f"📁 Using configuration directory: {self.config_dir}")
                        break
            
            # Fallback to config if nothing found
            if self.config_dir is None:
                self.config_dir = base_path / "config"
                logger.warning(f"⚠️ Using fallback config directory: {self.config_dir}")
        else:
            self.config_dir = Path(config_dir)
        
        # Configuration storage
        self.meta_config = {}
        self.date_rules = {}
        self.system_config = {}
        self.variable_definitions = {}
        
        # Load all configurations
        self._load_configurations()
        
    def _load_configurations(self):
        """Load all configuration files."""
        try:
            # Ensure config_dir is set and is a Path object
            if self.config_dir is None:
                raise RuntimeError("Configuration directory (config_dir) is not set. Cannot load configuration files.")
            meta_config_path = self.config_dir / "meta_config.json"
            if meta_config_path.exists():
                with open(meta_config_path, 'r', encoding='utf-8') as f:
                    self.meta_config = json.load(f)
                logger.info(f"✅ Loaded meta configuration from {meta_config_path}")
            else:
                # Fallback: try loading variables_config.json (legacy/simple config)
                variables_config_path = self.config_dir / "variables_config.json"
                if variables_config_path.exists():
                    try:
                        with open(variables_config_path, 'r', encoding='utf-8') as f:
                            self.meta_config = json.load(f)
                        logger.info(f"ℹ️ Loaded variables configuration from {variables_config_path} as meta_config fallback")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to load variables_config.json fallback: {e}")
            
            # Load date rules
            date_rules_path = self.config_dir / "date_rules_config.json"  
            if date_rules_path.exists():
                with open(date_rules_path, 'r', encoding='utf-8') as f:
                    self.date_rules = json.load(f)
                logger.info(f"✅ Loaded date rules from {date_rules_path}")
            
            # Load system configuration
            system_config_path = self.config_dir / "system_config.json"
            if system_config_path.exists():
                with open(system_config_path, 'r', encoding='utf-8') as f:
                    self.system_config = json.load(f)
                logger.info(f"✅ Loaded system configuration from {system_config_path}")
            
            # Load variable definitions
            var_def_path = self.config_dir / "variable_definitions.json"
            if var_def_path.exists():
                with open(var_def_path, 'r', encoding='utf-8') as f:
                    self.variable_definitions = json.load(f)
                logger.info(f"✅ Loaded variable definitions from {var_def_path}")
            
            # Log environment configuration for path resolution
            env_info = get_environment_info()
            logger.info(f"🌍 Environment: {env_info['ambiente']} | Parquet Home: {env_info['parquet_home']}")
            if env_info['is_s3']:
                logger.info(f"☁️ S3 Bucket: {env_info['s3a_bucket']}")
                    
        except Exception as e:
            logger.error(f"❌ Error loading configurations: {e}")
            raise
    
    def get_meta_config(self, meta_name: str) -> Dict[str, Any]:
        """Get configuration for a specific meta."""
        meta_key = meta_name.lower()
        if meta_key in self.meta_config:
            return self.meta_config[meta_key]
        else:
            logger.warning(f"⚠️ No configuration found for meta '{meta_name}'")
            return {}
    
    def get_variables(self, meta_name: str, variable_name: str) -> List[int]:
        """Get situacao codes for a specific meta and variable."""
        meta_config = self.get_meta_config(meta_name)
        
        # Try to get from meta_config variables
        if "variables" in meta_config and variable_name in meta_config["variables"]:
            var_config = meta_config["variables"][variable_name]
            if "case1" in var_config:
                return var_config["case1"]
            if "case2" in var_config:
                return var_config["case2"]
            if "case3" in var_config:
                return var_config["case3"]
        
        # Try to get from variable_definitions
        if meta_name in self.variable_definitions.get("variable_definitions", {}):
            meta_vars = self.variable_definitions["variable_definitions"][meta_name]
            if variable_name in meta_vars:
                var_def = meta_vars[variable_name]
                if "situacao_codes" in var_def:
                    # Handle both simple lists and complex dictionaries
                    codes = var_def["situacao_codes"]
                    if isinstance(codes, list):
                        return codes
                    elif isinstance(codes, dict):
                        # For complex cases like distribution/exclusion
                        if "distribution" in codes:
                            return codes["distribution"]
                        else:
                            # Return all codes from dict values that are lists
                            all_codes = []
                            for v in codes.values():
                                if isinstance(v, list):
                                    all_codes.extend(v)
                            return all_codes
        
        logger.warning(f"⚠️ No situacao codes found for {meta_name}.{variable_name}")
        return []
    
    def get_date_cutoff(self, meta_name: str, cutoff_type: str = "cutoff_date"):
        """Get date cutoff for a specific meta.

        Returns a datetime.datetime. If the configuration provides only a date
        (YYYY-MM-DD) the function will return a datetime at 00:00:00 for
        start-like cutoffs and 23:59:59 for end-like cutoffs (e.g., 'period_end').
        """
        # Try meta-specific configuration first
        meta_config = self.get_meta_config(meta_name)
        if "date_rules" in meta_config and cutoff_type in meta_config["date_rules"]:
            date_str = meta_config["date_rules"][cutoff_type]
            return self._parse_date_string(date_str, cutoff_type=cutoff_type)
        
        
    
    def get_available_situacoes(self, source: str = "ejud") -> List[int]:
        """Get list of available situacao codes."""
        if "available_situacoes" in self.system_config:
            available = self.system_config["available_situacoes"]
            if source in available:
                return available[source]
            elif "codes" in available:  # Fallback to generic codes
                return available["codes"]
        
        # Fallback to meta_config if available
        meta_config = self.get_meta_config("meta_02")  # Use meta_02 as reference
        if "situacao_rules" in meta_config and "available_situacoes" in meta_config["situacao_rules"]:
            return meta_config["situacao_rules"]["available_situacoes"]["codes"]
        
        logger.warning("⚠️ No available situacoes configuration found")
        return []
    
    def get_system_path(self, path_type: str) -> str:
        """Get system path configuration with environment-aware resolution."""
        if "system_paths" in self.system_config:
            paths = self.system_config["system_paths"]
            
            # Check in data_paths first
            if "data_paths" in paths and path_type in paths["data_paths"]:
                config_path = paths["data_paths"][path_type]
                resolved_path = resolve_path_config(config_path)
                logger.debug(f"Resolved path for '{path_type}': {resolved_path}")
                return resolved_path
            
            # Check in export_paths
            if "export_paths" in paths and path_type in paths["export_paths"]:
                config_path = paths["export_paths"][path_type]
                resolved_path = resolve_path_config(config_path)
                logger.debug(f"Resolved path for '{path_type}': {resolved_path}")
                return resolved_path
        
        logger.warning(f"⚠️ No path configuration found for '{path_type}'")
        return ""
    
    def get_export_path(self, meta_name: str) -> str:
        """
        Get export path for a specific meta.
        
        Args:
            meta_name: Meta name (e.g., "meta_02", "meta_03")
            
        Returns:
            str: Export path for the meta
        """
        config = self.get_meta_config(meta_name)
        if config and "paths" in config and "export_base_path" in config["paths"]:
            return config["paths"]["export_base_path"]
        # fallback: try at the root of the meta config
        if config and "export_base_path" in config:
            return config["export_base_path"]
        return ""  # Changed from None to empty string

    def get_processing_config(self, config_type: str) -> Dict[str, Any]:
        """Get processing configuration."""
        if "processing_config" in self.system_config:
            return self.system_config["processing_config"].get(config_type, {})
        
        # Fallback to meta_config
        meta_config = self.get_meta_config("meta_02")
        if "processing" in meta_config:
            return meta_config["processing"]
        
        return {}
    
    def get_variable_definition(self, meta_name: str, variable_name: str) -> Dict[str, Any]:
        """Get complete variable definition."""
        if meta_name in self.variable_definitions.get("variable_definitions", {}):
            meta_vars = self.variable_definitions["variable_definitions"][meta_name]
            if variable_name in meta_vars:
                return meta_vars[variable_name]
        
        logger.warning(f"⚠️ No variable definition found for {meta_name}.{variable_name}")
        return {}
    
    def _parse_date_string(self, date_str: str, cutoff_type: str = ""):
        """Parse date/time strings into a datetime.datetime.

        Supports:
        - "TODAY" -> today at 00:00:00
        - YYYY-MM-DD -> returns datetime at 00:00:00 for start-like cutoffs,
          or at 23:59:59 for end-like cutoffs
        - YYYY-MM-DD HH:MM:SS -> returns the exact datetime
        """
        if not date_str:
            return datetime.now()

        if isinstance(date_str, (int, float)):
            # Defensive: numeric timestamps not expected, return now
            return datetime.now()

        if date_str == "TODAY":
            # Return today's date at midnight
            today = datetime.combine(date.today(), datetime.min.time())
            return today

        # Identify if this cutoff should be treated as an "end of day" semantic
        cutoff_type_lower = (cutoff_type or "").lower()
        is_end_like = any(keyword in cutoff_type_lower for keyword in ["period_end", "_end", "cutoff_date", "_cutoff", "_end_date"]) or cutoff_type_lower.endswith("end")

        # Try to parse as YYYY-MM-DD HH:MM:SS first
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            # If it's an end-like cutoff and time is exactly midnight, normalize to 23:59:59
            if is_end_like and dt.time() == datetime.min.time():
                dt = dt.replace(hour=23, minute=59, second=59, microsecond=0)
            # Also if it's end-like and time is 00:00:01 etc (defensive) AND original date came without explicit time (can't detect here), we still keep user-supplied time
            return dt
        except Exception:
            pass

        # Try to parse as YYYY-MM-DD (date-only)
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            if is_end_like:
                # Always force 23:59:59 for end-like cutoffs (business rule)
                end_dt = datetime.combine(d, datetime.max.time()).replace(microsecond=0)
                # datetime.max.time() yields 23:59:59.999999, microsecond trimmed above
                return end_dt
            # Start-like cutoffs -> 00:00:00
            return datetime.combine(d, datetime.min.time())
        except Exception:
            pass

        logger.warning(f"⚠️ Could not parse date string: {date_str} - returning now()")
        return datetime.now()
    
    def get_exclusion_codes(self, meta_name: str, variable_name: str) -> List[int]:
        """Get exclusion situacao codes for distribution-with-exclusion variables."""
        
        # For Meta 03, check meta_config variables first to avoid unnecessary warnings
        if meta_name == "meta_03":
            meta_config = self.get_meta_config(meta_name)
            if "variables" in meta_config and variable_name in meta_config["variables"]:
                var_config = meta_config["variables"][variable_name]
                if "exclusion_codes" in var_config:
                    return var_config["exclusion_codes"]
        
        # Try variable_definitions for other metas
        var_def = self.get_variable_definition(meta_name, variable_name)
        
        if "situacao_codes" in var_def and isinstance(var_def["situacao_codes"], dict):
            return var_def["situacao_codes"].get("exclusion", [])
        
        # Try meta_config as fallback for other metas
        meta_config = self.get_meta_config(meta_name)
        if "variables" in meta_config and variable_name in meta_config["variables"]:
            var_config = meta_config["variables"][variable_name]
            if "exclusion_codes" in var_config:
                return var_config["exclusion_codes"]
        
        return []
    
    def should_apply_period_filter(self, meta_name: str, variable_name: str) -> bool:
        """Check if period filter should be applied for a variable."""
        var_def = self.get_variable_definition(meta_name, variable_name)
        
        if "date_filter" in var_def:
            return var_def["date_filter"].get("apply_period_filter", False)
        
        return False
    
    def get_date_column(self, meta_name: str, variable_name: str) -> str:
        """Get the date column to use for a variable."""
        var_def = self.get_variable_definition(meta_name, variable_name)
        
        if "date_filter" in var_def:
            return var_def["date_filter"].get("date_column", "dthrmov")
        
        return "dthrmov"  # Default
    
    def should_apply_eligible_filter(self, meta_name: str, variable_name: str) -> bool:
        """Check if eligible process filter should be applied."""
        var_def = self.get_variable_definition(meta_name, variable_name)
        
        if "process_filter" in var_def:
            return var_def["process_filter"].get("apply_eligible_filter", True)
        
        return True  # Default to applying filter
    
    def print_configuration_summary(self, meta_name: Optional[str] = None):
        """Print a summary of loaded configuration."""
        print("\n" + "="*60)
        print("CONFIGURATION SUMMARY")
        print("="*60)
        
        if meta_name:
            meta_config = self.get_meta_config(meta_name)
            if meta_config:
                print(f"\n📋 {meta_name.upper()} Configuration:")
                
                # Date rules
                if "date_rules" in meta_config:
                    print(f"  📅 Date Rules:")
                    for key, value in meta_config["date_rules"].items():
                        print(f"    - {key}: {value}")
                
                # Situacao rules  
                if "situacao_rules" in meta_config:
                    print(f"  🔢 Situacao Rules:")
                    for rule_name, rule_data in meta_config["situacao_rules"].items():
                        if "codes" in rule_data:
                            print(f"    - {rule_name}: {rule_data['codes']}")
                
                # Variables
                if "variables" in meta_config:
                    print(f"  📊 Variables:")
                    for var_name in meta_config["variables"].keys():
                        print(f"    - {var_name}")
        else:
            print(f"\n📁 Configuration Files Loaded:")
            print(f"  - Meta Config: {bool(self.meta_config)}")
            print(f"  - Date Rules: {bool(self.date_rules)}")
            print(f"  - System Config: {bool(self.system_config)}")
            print(f"  - Variable Definitions: {bool(self.variable_definitions)}")
        
        print("\n" + "="*60)
    
    def get_meta_path(self, meta_name: str, path_type: str):
        """
        Get Meta-specific path configuration.
        
        Args:
            meta_name: Meta name (e.g., "meta_03", "meta_02")
            path_type: Type of path to retrieve (e.g., "filter_file", "data_path", "export_path", "meta04_filter_paths")
            
        Returns:
            str or List[str]: Path(s) for the specified meta and path type
        """
        # Ensure meta_name format (e.g., "meta_03")
        if not meta_name.startswith("meta_"):
            meta_name = f"meta_{meta_name.zfill(2)}"
        
        # Try to get from meta-specific configuration first
        meta_config = self.get_meta_config(meta_name)
          # PRIORITY 1: Check in "paths" section (new structure)
        if "paths" in meta_config and path_type in meta_config["paths"]:
            path = meta_config["paths"][path_type]
            logger.info(f"✅ Found {path_type} for {meta_name} in paths section: {path}")
            
            # Handle list of paths (e.g., meta04_filter_paths)
            if isinstance(path, list):
                # Convert relative paths to absolute paths
                workspace_root = Path(__file__).parent.parent
                absolute_paths = []
                for p in path:
                    if os.path.isabs(p):
                        absolute_paths.append(p)
                    else:
                        absolute_paths.append(str(workspace_root / p))
                return absolute_paths
            
            return path
        
        # PRIORITY 2: Check in filter_rules for filter-related paths
        if "filter_rules" in meta_config and path_type in meta_config["filter_rules"]:
            filter_config = meta_config["filter_rules"][path_type]
            if isinstance(filter_config, dict) and "path" in filter_config:
                return filter_config["path"]
            elif isinstance(filter_config, str):
                return filter_config
        
        # PRIORITY 3: Check in processing configuration
        if "processing" in meta_config and "paths" in meta_config["processing"]:
            paths = meta_config["processing"]["paths"]
            if path_type in paths:
                return paths[path_type]
        
        # Check in system_paths for meta-specific paths
        if "system_paths" in self.system_config:
            paths = self.system_config["system_paths"]
            
            # Check in data_paths with meta prefix
            if "data_paths" in paths:
                meta_path_key = f"{meta_name}_{path_type}"
                if meta_path_key in paths["data_paths"]:
                    return paths["data_paths"][meta_path_key]
                elif path_type in paths["data_paths"]:
                    return paths["data_paths"][path_type]
            
            # Check in export_paths
            if "export_paths" in paths and meta_name in paths["export_paths"]:
                if path_type == "export_path":
                    return paths["export_paths"][meta_name]
        
        # Fallback: construct default paths based on path_type
        if path_type == "filter_file" and meta_name == "meta_03":
            base_path = self.get_system_path("situacao_base_path")
            if base_path:
                return f"{base_path}/filtered_coddoc_meta03.parquet"
            else:
                return "/opt/parquet/data/prata/ejud/situacao_cnj2026/filtered_coddoc_meta03.parquet"
        
        elif path_type == "export_path":
            return self.get_export_path(meta_name)
        
        elif path_type == "data_path":
            base_path = self.get_system_path("situacao_base_path")
            if base_path:
                return f"{base_path}/{meta_name}"
            else:
                return f"/opt/parquet/data/prata/ejud/situacao_cnj2026/{meta_name}"
        
        logger.warning(f"⚠️ No meta path found for '{meta_name}.{path_type}', returning empty string")
        return ""

# Example usage and testing
if __name__ == "__main__":
    # Test the configuration manager
    config_manager = MetaConfigManager()
    
    # Test meta_02 configuration
    print("\n🧪 TESTING META 02 CONFIGURATION")
    
    # Test date cutoffs
    cutoff_date = config_manager.get_date_cutoff("meta_02", "meta02_cutoff_date")
    print(f"Meta 02 cutoff date: {cutoff_date}")
    
    long_term_cutoff = config_manager.get_date_cutoff("meta_02", "meta02_15year_cutoff")
    print(f"Meta 02 15-year cutoff: {long_term_cutoff}")
    
    # Test situacao codes
    dism2_codes = config_manager.get_variables("meta_02", "dism2")
    print(f"DISM2 situacao codes: {dism2_codes}")
    
    exclusion_codes = config_manager.get_exclusion_codes("meta_02", "dism2")
    print(f"DISM2 exclusion codes: {exclusion_codes}")
    
    # Test paths
    base_path = config_manager.get_system_path("situacao_base_path")
    print(f"Situacao base path: {base_path}")
    
    export_path = config_manager.get_system_path("meta_02")
    print(f"Meta 02 export path: {export_path}")
    
    # Test available situacoes
    available = config_manager.get_available_situacoes("ejud")
    print(f"Available EJUD situacoes: {available}")
    
    # Print configuration summary
    config_manager.print_configuration_summary("meta_02")
