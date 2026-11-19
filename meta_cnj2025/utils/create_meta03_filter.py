#!/usr/bin/env python3
"""
Meta 03 Filter Generator
Creates filtered parquet for Meta 03 based on Excel classes file and EJUD data.

Process:
1. Read classes.xlsx from csv_files folder
2. Load t_processo parquet files from bronze layer
3. Load tipoprocesso parquet files from bronze layer
4. Join Excel -> tipoprocesso -> t_processo
5. Save filtered coddoc list to parquet_files folder

Usage:
    python utils/create_meta03_filter.py
"""

import os
import sys
import glob
import pandas as pd
import polars as pl
from pathlib import Path
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Meta03FilterGenerator:
    """Generator for Meta 03 filtered process data."""
    
    def __init__(self):
        """Initialize the generator with paths."""
        self.workspace_root = Path(__file__).parent.parent
        self.csv_files_path = self.workspace_root / "csv"
        self.parquet_files_path = self.workspace_root / "parquet_files"
        self.excel_file_path = self.csv_files_path / "classes.xlsx"
        
        # Bronze layer paths for EJUD data
        self.t_processo_base_path = "/opt/parquet/data/bronze/ejud/t_processo"
        self.tipoprocesso_base_path = "/opt/parquet/data/bronze/ejud/tipoprocesso"
        
        # Output file
        self.output_file = self.parquet_files_path / "filtered_coddoc_meta03.parquet"
        
        logger.info(f"📁 Workspace root: {self.workspace_root}")
        logger.info(f"📊 Excel file: {self.excel_file_path}")
        logger.info(f"💾 Output file: {self.output_file}")
    
    def read_excel_classes(self) -> pd.DataFrame:
        """
        Read classes.xlsx file and extract required columns.
        
        Returns:
            DataFrame with codigo and tipo_procedimento columns
        """
        logger.info("📖 Reading classes.xlsx file...")
        
        if not self.excel_file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.excel_file_path}")
        
        try:
            # Read Excel file
            df_excel = pd.read_excel(self.excel_file_path)
            logger.info(f"📄 Excel file loaded: {len(df_excel)} rows, {len(df_excel.columns)} columns")
            logger.info(f"📋 Available columns: {df_excel.columns.tolist()}")
            
            # Check for required columns
            required_columns = ["Código", "Tipo de Procedimento"]
            missing_columns = [col for col in required_columns if col not in df_excel.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns in Excel: {missing_columns}")
            
            # Select and rename columns
            df_filtered = df_excel[["Código", "Tipo de Procedimento"]].copy()
            df_filtered = df_filtered.rename(columns={
                "Código": "codigo",
                "Tipo de Procedimento": "tipo_procedimento"
            })
              # Clean data
            df_filtered = df_filtered.dropna(subset=["codigo"])
            
            # Convert codigo to string, handling both int and float formats
            # Remove any decimal points and trailing zeros (e.g., "123.0" -> "123")
            df_filtered["codigo"] = df_filtered["codigo"].apply(
                lambda x: str(int(float(x))) if pd.notna(x) else str(x)
            ).str.strip()
            
            df_filtered["tipo_procedimento"] = df_filtered["tipo_procedimento"].astype(str).str.strip()
            logger.info(f"✅ Excel data processed: {len(df_filtered)} valid records")
            logger.info(f"📊 Sample codes: {df_filtered['codigo'].head().tolist()}")
            logger.info(f"🔍 Excel codigo data types: {df_filtered['codigo'].dtype}")
            
            # Return just the tipo_procedimento is equal to 'Conhecimento'
            df_filtered = df_filtered[df_filtered["tipo_procedimento"] == "CONHECIMENTO"]
            
            return df_filtered
            
        except Exception as e:
            logger.error(f"❌ Failed to read Excel file: {e}")
            raise
    
    def load_parquet_files(self, base_path: str, required_columns: List[str]) -> pl.DataFrame:
        """
        Load parquet files from a base path, handling subfolders.
        
        Args:
            base_path: Base directory path
            required_columns: List of required column names
            
        Returns:
            Combined Polars DataFrame
        """
        logger.info(f"📂 Loading parquet files from: {base_path}")
        
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Path not found: {base_path}")
        
        # Find all parquet files recursively
        pattern = os.path.join(base_path, "**", "*.parquet")
        parquet_files = glob.glob(pattern, recursive=True)
        
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in: {base_path}")
        
        logger.info(f"📄 Found {len(parquet_files)} parquet files")
        
        # Load files
        dfs = []
        loaded_files = 0
        
        for file_path in parquet_files:
            try:
                # Check if required columns exist
                df_temp = pl.read_parquet(file_path, n_rows=0)  # Read just schema
                available_columns = df_temp.columns
                
                missing_columns = [col for col in required_columns if col not in available_columns]
                if missing_columns:
                    logger.warning(f"⚠️ Skipping {file_path} - missing columns: {missing_columns}")
                    continue
                
                # Load with required columns only
                df = pl.read_parquet(file_path, columns=required_columns)
                if len(df) > 0:
                    dfs.append(df)
                    loaded_files += 1
                    
            except Exception as e:
                logger.warning(f"⚠️ Failed to load {file_path}: {e}")
                continue
        
        if not dfs:
            raise ValueError(f"No valid parquet files loaded from {base_path}")
        
        # Combine all DataFrames
        combined_df = pl.concat(dfs, how="vertical")
        logger.info(f"✅ Loaded {loaded_files} files, {len(combined_df)} total records")
        
        return combined_df
    
    def load_tipoprocesso_data(self) -> pl.DataFrame:
        """
        Load tipoprocesso data with required columns.
        
        Returns:
            DataFrame with codtipproc and codextcnj columns
        """
        logger.info("🔧 Loading tipoprocesso data...")
        
        required_columns = ["codtipproc", "codextcnj", "codnat"]
        df_tipoprocesso = self.load_parquet_files(self.tipoprocesso_base_path, required_columns)
        
        # Clean and deduplicate
        # Be permissive about codnat type: accept numeric 1 or string '1'
        try:
            # Log sample codnat values for debugging mismatches
            if 'codnat' in df_tipoprocesso.columns:
                unique_codnat = df_tipoprocesso.select(pl.col('codnat')).unique().limit(20)
                try:
                    sample_list = unique_codnat['codnat'].to_list()
                except Exception:
                    sample_list = []
                logger.info(f"🔍 Tipoprocesso codnat sample values (up to 20): {sample_list}")
        except Exception:
            logger.debug('Could not sample codnat values')

        df_tipoprocesso = df_tipoprocesso.filter(
            pl.col("codtipproc").is_not_null() & 
            pl.col("codextcnj").is_not_null() & 
            (pl.col("codnat").cast(pl.Utf8) == '1')
        )
          # Convert codextcnj to string for joining, handling float format
        # Remove decimal points and trailing zeros (e.g., "123.0" -> "123")
        # Normalize codextcnj to string and strip trailing .0 produced by some writers
        df_tipoprocesso = df_tipoprocesso.with_columns(
            pl.col("codextcnj").cast(pl.Utf8).str.replace(r"\.0+$", "").alias("codextcnj")
        )
        
        # Remove duplicates
        df_tipoprocesso = df_tipoprocesso.unique(subset=["codtipproc"])
        logger.info(f"🔧 Tipoprocesso data: {len(df_tipoprocesso)} unique records")
        logger.info(f"📊 Sample codextcnj values: {df_tipoprocesso['codextcnj'].head().to_list()}")
        logger.info(f"🔍 Tipoprocesso codextcnj data types: {df_tipoprocesso['codextcnj'].dtype}")
        # Normalize codtipproc to string for downstream joins
        if 'codtipproc' in df_tipoprocesso.columns:
            try:
                df_tipoprocesso = df_tipoprocesso.with_columns(pl.col('codtipproc').cast(pl.Utf8).alias('codtipproc'))
            except Exception:
                logger.warning('⚠️ Could not cast tipoprocesso.codtipproc to string')
        
        return df_tipoprocesso
    
    def load_t_processo_data(self) -> pl.DataFrame:
        """
        Load t_processo data with required columns.
        
        Returns:
            DataFrame with coddoc and codtipproc columns
        """
        logger.info("📋 Loading t_processo data...")
        
        required_columns = ["coddoc", "codtipproc"]
        df_t_processo = self.load_parquet_files(self.t_processo_base_path, required_columns)
        
        # Clean data
        df_t_processo = df_t_processo.filter(
            pl.col("coddoc").is_not_null() & 
            pl.col("codtipproc").is_not_null()
        )
        
        # Remove duplicates
        df_t_processo = df_t_processo.unique(subset=["coddoc"])
        
        logger.info(f"📋 T_processo data: {len(df_t_processo)} unique records")
        # Normalize codtipproc to string to match tipoprocesso
        if 'codtipproc' in df_t_processo.columns:
            try:
                df_t_processo = df_t_processo.with_columns(pl.col('codtipproc').cast(pl.Utf8).alias('codtipproc'))
            except Exception:
                logger.warning('⚠️ Could not cast t_processo.codtipproc to string')

        # Ensure coddoc is integer-like
        if 'coddoc' in df_t_processo.columns:
            try:
                df_t_processo = df_t_processo.with_columns(pl.col('coddoc').cast(pl.Int64).alias('coddoc'))
            except Exception:
                logger.warning('⚠️ Could not cast t_processo.coddoc to Int64')
        
        return df_t_processo
    
    def perform_joins(self, df_excel: pd.DataFrame, df_tipoprocesso: pl.DataFrame, 
                     df_t_processo: pl.DataFrame) -> pl.DataFrame:
        """
        Perform the complex join between Excel, tipoprocesso, and t_processo.
        
        Args:
            df_excel: Excel data with codigo and tipo_procedimento
            df_tipoprocesso: Tipoprocesso data with codtipproc and codextcnj
            df_t_processo: T_processo data with coddoc and codtipproc
            
        Returns:
            Final DataFrame with filtered processes
        """
        logger.info("🔗 Performing joins...")
        
        # Convert Excel DataFrame to Polars
        df_excel_pl = pl.from_pandas(df_excel)
        
        logger.info(f"📊 Starting join process:")
        logger.info(f"   - Excel: {len(df_excel_pl)} records")
        logger.info(f"   - Tipoprocesso: {len(df_tipoprocesso)} records")
        logger.info(f"   - T_processo: {len(df_t_processo)} records")
        
        # Step 1: Join Excel with tipoprocesso (codigo = codextcnj)
        df_step1 = df_excel_pl.join(
            df_tipoprocesso,
            left_on="codigo",
            right_on="codextcnj",
            how="inner"
        )
        
        logger.info(f"🔗 After Excel -> Tipoprocesso join: {len(df_step1)} records")
        
        if len(df_step1) == 0:
            logger.warning("⚠️ No matches found in Excel -> Tipoprocesso join")
            logger.info("🔍 Debugging join:")
            excel_codes = set(df_excel_pl["codigo"].to_list())
            tipo_codes = set(df_tipoprocesso["codextcnj"].to_list())
            intersection = excel_codes.intersection(tipo_codes)
            logger.info(f"   - Common codes: {len(intersection)} out of {len(excel_codes)} Excel codes")
        
        # Step 2: Join result with t_processo (codtipproc = codtipproc)
        try:
            if 'codtipproc' in df_step1.columns:
                df_step1 = df_step1.with_columns(pl.col('codtipproc').cast(pl.Utf8).alias('codtipproc'))
            if 'codtipproc' in df_t_processo.columns:
                df_t_processo = df_t_processo.with_columns(pl.col('codtipproc').cast(pl.Utf8).alias('codtipproc'))
        except Exception:
            logger.warning('⚠️ Failed to normalize codtipproc types before join; attempting join anyway')

        df_final = df_step1.join(
            df_t_processo,
            on="codtipproc",
            how="inner"
        )
        
        logger.info(f"🔗 After Tipoprocesso -> T_processo join: {len(df_final)} records")
        
        # Select final columns - for Meta 03 filter, we only need coddoc
        df_result = df_final.select([
            "coddoc"
        ]).unique()  # Remove any duplicate coddoc values
        
        logger.info(f"✅ Final result: {len(df_result)} unique processes for Meta 03")
        
        return df_result
    
    def save_results(self, df: pl.DataFrame) -> str:
        """
        Save the filtered results to parquet file.
        
        Args:
            df: DataFrame to save
            
        Returns:
            Path to saved file
        """
        logger.info(f"💾 Saving results to: {self.output_file}")
        
        # Create output directory if needed
        self.parquet_files_path.mkdir(exist_ok=True)
        
        # Save to parquet
        df.write_parquet(self.output_file)
        
        logger.info(f"✅ Results saved: {len(df)} unique coddoc records")
        logger.info(f"📁 File location: {self.output_file}")
        
        # Show basic statistics
        logger.info(f"📊 Saved {len(df)} unique coddoc values for Meta 03 filtering")
        
        return str(self.output_file)
    
    def generate_filter(self) -> Optional[str]:
        """
        Main method to generate the Meta 03 filter.
        
        Returns:
            Path to generated parquet file
        """
        logger.info("🚀 Starting Meta 03 filter generation...")
        
        try:
            # Step 1: Read Excel file
            df_excel = self.read_excel_classes()
            
            # Step 2: Load tipoprocesso data
            df_tipoprocesso = self.load_tipoprocesso_data()
            
            # Step 3: Load t_processo data
            df_t_processo = self.load_t_processo_data()
            
            # Step 4: Perform joins
            df_result = self.perform_joins(df_excel, df_tipoprocesso, df_t_processo)
            
            if len(df_result) == 0:
                logger.warning("⚠️ No results after joins - check data compatibility")
                return None
            
            # Step 5: Save results
            output_path = self.save_results(df_result)
            
            logger.info("🎉 Meta 03 filter generation completed successfully!")
            
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Filter generation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

def main():
    """Main execution function."""
    try:
        generator = Meta03FilterGenerator()
        output_path = generator.generate_filter()
        
        if output_path:
            print(f"✅ Meta 03 filter created successfully!")
            print(f"📁 Output file: {output_path}")
            return 0
        else:
            print("❌ No results generated")
            return 1
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
