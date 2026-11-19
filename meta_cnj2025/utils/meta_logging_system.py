import os
import json
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import traceback
from contextlib import contextmanager

class MetaLogger:
    """
    Enhanced logging system for Meta CNJ 2026 calculations.
    
    Provides structured logging, performance tracking, and error monitoring
    specifically designed for Meta calculations.
    """
    
    def __init__(self, meta_number: str, logs_base_path: str = "logs"):
        """
        Initialize Meta Logger.
        
        Args:
            meta_number: Meta number (e.g., "02", "03", "04")
            logs_base_path: Base directory for log files
        """
        self.meta_number = meta_number.zfill(2)
        self.logs_base_path = logs_base_path
        self.logs_dir = f"{logs_base_path}/meta_{self.meta_number}"
        
        # Create logs directory
        self._ensure_log_directories()
        
        # Setup loggers
        self.logger = self._setup_main_logger()
        self.performance_logger = self._setup_performance_logger()
        self.error_logger = self._setup_error_logger()
        
        # Session tracking
        self.session_start = datetime.now()
        self.variables_completed = []
        self.errors_count = 0
        self.warnings_count = 0
        
        self.logger.info(f"🚀 MetaLogger initialized for Meta {self.meta_number}")
    
    def _ensure_log_directories(self):
        """Create necessary log directories."""
        try:
            Path(self.logs_dir).mkdir(parents=True, exist_ok=True)
            Path(f"{self.logs_dir}/archive").mkdir(exist_ok=True)
            Path(f"{self.logs_dir}/performance").mkdir(exist_ok=True)
            Path(f"{self.logs_dir}/errors").mkdir(exist_ok=True)
        except Exception as e:
            print(f"❌ Failed to create log directories: {e}")
            raise
    
    def _setup_main_logger(self) -> logging.Logger:
        """Setup main application logger with rotation."""
        logger = logging.getLogger(f"meta_{self.meta_number}")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # File handler with rotation (10MB per file, keep 5 files)
        log_file = f"{self.logs_dir}/meta_{self.meta_number}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _setup_performance_logger(self) -> logging.Logger:
        """Setup performance metrics logger."""
        logger = logging.getLogger(f"meta_{self.meta_number}_performance")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        # Daily rotation for performance logs
        log_file = f"{self.logs_dir}/performance/performance_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        
        # JSON formatter for structured performance data
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        return logger
    
    def _setup_error_logger(self) -> logging.Logger:
        """Setup error tracking logger."""
        logger = logging.getLogger(f"meta_{self.meta_number}_errors")
        logger.setLevel(logging.ERROR)
        logger.handlers.clear()
        
        # Error log with rotation
        log_file = f"{self.logs_dir}/errors/errors_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=10
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n%(pathname)s:%(lineno)d\n'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        return logger
    
    def log_calculation_start(self, description: str = None):
        """Log the start of Meta calculation."""
        self.session_start = datetime.now()
        
        start_info = {
            "event": "calculation_start",
            "meta_number": self.meta_number,
            "timestamp": self.session_start.isoformat(),
            "description": description or f"Meta {self.meta_number} calculation started"
        }
        
        self.logger.info(f"🚀 Starting Meta {self.meta_number} calculation")
        if description:
            self.logger.info(f"📝 Description: {description}")
        
        # Log structured data for analysis
        self.performance_logger.info(json.dumps(start_info))
    
    def log_variable_start(self, variable_name: str, description: str = None):
        """Log the start of a variable calculation."""
        start_time = datetime.now()
        
        self.logger.info(f"📊 Starting calculation: {variable_name}")
        if description:
            self.logger.info(f"📝 {variable_name} description: {description}")
        
        # Store start time for duration calculation
        setattr(self, f"_{variable_name}_start_time", start_time)
    
    def log_variable_completion(self, variable_name: str, record_count: int, duration_seconds: float = None, 
                              additional_metrics: Dict = None):
        """Log the completion of a variable calculation."""
        end_time = datetime.now()
        
        # Calculate duration if not provided
        if duration_seconds is None:
            start_time = getattr(self, f"_{variable_name}_start_time", end_time)
            duration_seconds = (end_time - start_time).total_seconds()
        
        # Calculate performance metrics
        records_per_second = record_count / duration_seconds if duration_seconds > 0 else 0
        
        completion_info = {
            "event": "variable_completion",
            "meta_number": self.meta_number,
            "variable_name": variable_name,
            "record_count": record_count,
            "duration_seconds": round(duration_seconds, 2),
            "records_per_second": round(records_per_second, 2),
            "timestamp": end_time.isoformat(),
            "additional_metrics": additional_metrics or {}
        }
        
        self.logger.info(f"✅ {variable_name} completed: {record_count:,} records in {duration_seconds:.2f}s ({records_per_second:.0f} rec/sec)")
        
        # Log structured performance data
        self.performance_logger.info(json.dumps(completion_info))
        
        # Track for session summary
        self.variables_completed.append(completion_info)
    
    def log_error(self, error: Exception, context: str = None, critical: bool = False):
        """Log an error with full context and stack trace."""
        self.errors_count += 1
        
        error_info = {
            "event": "error",
            "meta_number": self.meta_number,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "critical": critical,
            "timestamp": datetime.now().isoformat(),
            "stack_trace": traceback.format_exc()
        }
        
        if critical:
            self.logger.error(f"🚨 CRITICAL ERROR in {context}: {error}")
        else:
            self.logger.error(f"❌ ERROR in {context}: {error}")
        
        # Log to error logger with full details
        self.error_logger.error(json.dumps(error_info, indent=2))
    
    def log_warning(self, message: str, context: str = None):
        """Log a warning message."""
        self.warnings_count += 1
        
        warning_info = {
            "event": "warning",
            "meta_number": self.meta_number,
            "message": message,
            "context": context,
            "timestamp": datetime.now().isoformat()
        }
        
        self.logger.warning(f"⚠️ WARNING: {message}")
        if context:
            self.logger.warning(f"📍 Context: {context}")
        
        # Log structured warning data
        self.performance_logger.info(json.dumps(warning_info))
    
    def log_memory_usage(self, stage: str, memory_mb: float = None):
        """Log memory usage at different calculation stages."""
        if memory_mb is None:
            try:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
            except ImportError:
                memory_mb = 0
        
        memory_info = {
            "event": "memory_usage",
            "meta_number": self.meta_number,
            "stage": stage,
            "memory_mb": round(memory_mb, 2),
            "timestamp": datetime.now().isoformat()
        }
        
        self.logger.info(f"🧠 Memory usage at {stage}: {memory_mb:.1f} MB")
        self.performance_logger.info(json.dumps(memory_info))
    
    def log_calculation_end(self, success: bool = True, summary: Dict = None):
        """Log the end of Meta calculation with summary."""
        end_time = datetime.now()
        total_duration = (end_time - self.session_start).total_seconds()
        
        # Calculate summary statistics
        total_records = sum(var.get("record_count", 0) for var in self.variables_completed)
        avg_records_per_second = sum(var.get("records_per_second", 0) for var in self.variables_completed) / len(self.variables_completed) if self.variables_completed else 0
        
        completion_info = {
            "event": "calculation_end",
            "meta_number": self.meta_number,
            "success": success,
            "total_duration_seconds": round(total_duration, 2),
            "variables_completed": len(self.variables_completed),
            "total_records_processed": total_records,
            "average_records_per_second": round(avg_records_per_second, 2),
            "errors_count": self.errors_count,
            "warnings_count": self.warnings_count,
            "timestamp": end_time.isoformat(),
            "summary": summary or {},
            "variables_detail": self.variables_completed
        }
        
        if success:
            self.logger.info(f"🎉 Meta {self.meta_number} calculation completed successfully!")
        else:
            self.logger.error(f"❌ Meta {self.meta_number} calculation failed!")
        
        self.logger.info(f"⏱️ Total duration: {total_duration:.2f} seconds")
        self.logger.info(f"📊 Variables completed: {len(self.variables_completed)}")
        self.logger.info(f"📈 Total records processed: {total_records:,}")
        self.logger.info(f"🚨 Errors: {self.errors_count}, Warnings: {self.warnings_count}")
        
        # Log structured completion data
        self.performance_logger.info(json.dumps(completion_info))
        
        # Generate session summary report
        self._generate_session_summary(completion_info)
    
    def log_calculation_completion(self, results: Dict, total_time: float):
        """
        Log the completion of the entire Meta calculation with comprehensive results.
        This is an alias for log_calculation_end with specific formatting for final results.
        
        Args:
            results: Dictionary with variable names as keys and record counts as values
            total_time: Total calculation time in seconds
        """
        # Calculate summary statistics
        total_records = sum(results.values())
        successful_variables = [var for var, count in results.items() if count > 0]
        failed_variables = [var for var, count in results.items() if count == 0]
        
        # Create comprehensive summary
        summary = {
            "total_records_processed": total_records,
            "total_duration_seconds": round(total_time, 2),
            "variables_calculated": len(results),
            "successful_variables": len(successful_variables),
            "failed_variables": len(failed_variables),
            "records_by_variable": results,
            "success_rate": len(successful_variables) / len(results) * 100 if results else 0,
            "average_records_per_variable": total_records / len(successful_variables) if successful_variables else 0,
            "calculation_rate": total_records / total_time if total_time > 0 else 0
        }
        
        # Log detailed completion information
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"🎉 META {self.meta_number.upper()} CALCULATION COMPLETED")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"⏱️ Total Time: {total_time:.2f} seconds")
        self.logger.info(f"📊 Total Records: {total_records:,}")
        self.logger.info(f"🎯 Variables Calculated: {len(results)}")
        self.logger.info(f"✅ Successful: {len(successful_variables)}")
        self.logger.info(f"❌ Failed: {len(failed_variables)}")
        self.logger.info(f"📈 Success Rate: {summary['success_rate']:.1f}%")
        self.logger.info(f"🚀 Processing Rate: {summary['calculation_rate']:.0f} records/second")
        
        # Log individual variable results
        self.logger.info(f"\n📋 Individual Variable Results:")
        for variable, count in results.items():
            status = "✅" if count > 0 else "❌"
            self.logger.info(f"  {status} {variable}: {count:,} records")
        
        if failed_variables:
            self.logger.warning(f"\n⚠️ Failed Variables: {', '.join(failed_variables)}")
        
        # Call the standard log_calculation_end method
        success = len(failed_variables) == 0  # Success if no failed variables
        self.log_calculation_end(success=success, summary=summary)
    
    def _generate_session_summary(self, completion_info: Dict):
        """Generate a session summary report."""
        try:
            summary_file = f"{self.logs_dir}/session_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(summary_file, 'w') as f:
                json.dump(completion_info, f, indent=2)
            
            self.logger.info(f"📄 Session summary saved: {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate session summary: {e}")
    
    @contextmanager
    def log_variable_context(self, variable_name: str, description: str = None):
        """Context manager for logging variable calculations."""
        try:
            self.log_variable_start(variable_name, description)
            start_time = datetime.now()
            yield
            # Note: log_variable_completion should be called manually with record count
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.log_error(e, f"Variable calculation: {variable_name}", critical=True)
            raise
    
    def get_session_summary(self) -> Dict:
        """Get current session summary."""
        current_time = datetime.now()
        duration = (current_time - self.session_start).total_seconds()
        
        return {
            "meta_number": self.meta_number,
            "session_start": self.session_start.isoformat(),
            "current_duration_seconds": round(duration, 2),
            "variables_completed": len(self.variables_completed),
            "errors_count": self.errors_count,
            "warnings_count": self.warnings_count,
            "log_directory": self.logs_dir
        }

# Convenience function for quick logger setup
def setup_meta_logger(meta_number: str, logs_base_path: str = "logs") -> MetaLogger:
    """
    Convenience function to setup a Meta logger.
    
    Args:
        meta_number: Meta number (e.g., "02", "03")
        logs_base_path: Base directory for logs
        
    Returns:
        MetaLogger: Configured logger instance
        
    Example:
        logger = setup_meta_logger("02")
        logger.log_calculation_start("Daily Meta 02 calculation")
    """
    return MetaLogger(meta_number, logs_base_path)

# Log analysis utilities
def analyze_meta_logs(meta_number: str, logs_base_path: str = "logs") -> Dict:
    """
    Analyze Meta calculation logs and generate insights.
    
    Args:
        meta_number: Meta number to analyze
        logs_base_path: Base directory for logs
        
    Returns:
        Dict: Analysis results and insights
    """
    try:
        logs_dir = f"{logs_base_path}/meta_{meta_number.zfill(2)}"
        performance_dir = f"{logs_dir}/performance"
        
        if not os.path.exists(performance_dir):
            return {"error": "No performance logs found"}
        
        # Analyze performance logs
        performance_files = [f for f in os.listdir(performance_dir) if f.startswith("performance_")]
        
        total_calculations = 0
        total_records = 0
        average_duration = 0
        error_count = 0
        
        for file in performance_files:
            file_path = f"{performance_dir}/{file}"
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            if data.get("event") == "calculation_end":
                                total_calculations += 1
                                total_records += data.get("total_records_processed", 0)
                                average_duration += data.get("total_duration_seconds", 0)
                                error_count += data.get("errors_count", 0)
                        except json.JSONDecodeError:
                            continue
            except Exception:
                continue
        
        if total_calculations > 0:
            average_duration = average_duration / total_calculations
        
        return {
            "meta_number": meta_number,
            "total_calculations": total_calculations,
            "total_records_processed": total_records,
            "average_duration_seconds": round(average_duration, 2),
            "total_errors": error_count,
            "logs_directory": logs_dir
        }
        
    except Exception as e:
        return {"error": f"Failed to analyze logs: {e}"}

if __name__ == "__main__":
    print("Meta Logging System - Ready for integration")
    print("Usage examples:")
    print("1. logger = setup_meta_logger('02')")
    print("2. logger.log_calculation_start()")
    print("3. logger.log_variable_completion('dism2', 1500, 45.2)")
    print("4. logger.log_calculation_end(success=True)")
