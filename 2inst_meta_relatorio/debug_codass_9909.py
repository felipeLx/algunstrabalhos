#!/usr/bin/env python3
"""
Debug script to verify if bronze parquet files have all expected records
and compare with filtered files for meta 11.

This script replicates the Postgres query in PySpark to check:
1. If bronze parquets contain all expected records
2. If filtered_coddoc_idoso.parquet has all the coddoc values
3. Identify any missing records or partition issues
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number, lit, count, countDistinct
from pyspark.sql.window import Window
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.parquet_utils import get_bronze_path, parquet_path

def create_spark_session():
    """Create Spark session with proper configuration"""
    return SparkSession.builder \
        .appName("Debug_Codass_9909_Bronze") \
        .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def run_query_on_bronze(spark):
    """
    Replicate the Postgres query on bronze parquet files
    
    Original query:
    select distinct m.coddoc, m.dthrmov, m.codfase, p.codass, a.descr 
    , coalesce(mj.codfase, md.codfase, mo.codfase) as fase_baixa
    , row_number() over(partition by m.coddoc order by m.dthrmov desc) rn
    from ejud.movimento m 
    left join dw2inst.tab_judmigracao tj on m.coddoc = tj.coddoc
    join ejud.processoassunto p on m.coddoc = p.coddoc 
    join ejud.assunto a on p.codass = a.codass 
    left join ejud.movjulg mj on m.coddoc = mj.coddoc 
        and mj.dthrmov < '2025-01-01 00:00'
    left join ejud.movdespa md on m.coddoc = md.coddoc 
        and md.dthrmov < '2025-01-01 00:00'
    left join ejud.movimento mo on m.coddoc = mo.coddoc 
        and mo.codfase in (246,22,846,870,897,861) 
        and mo.dthrmov < '2025-01-01 00:00'
    where m.codfase in (26, 36)
    and a.codass in (11832, 10951, 3659, 9909, 14692, 14693, 11947)
    and mj.coddoc is null and mo.coddoc is null and tj.coddoc is null
    and md.coddoc is null
    and m.dthrmov < '2025-01-01 00:00'
    """
    
    print("="*80)
    print("STEP 1: Loading Bronze Tables")
    print("="*80)
    
    # Load bronze tables
    try:
        movimento_path = get_bronze_path("ejud", "movimento")
        print(f"📂 Loading movimento from: {movimento_path}")
        movimento = spark.read.parquet(movimento_path)
        print(f"  ✅ Loaded {movimento.count():,} records")
        print(f"  📊 Partitions: {movimento.rdd.getNumPartitions()}")
        
    except Exception as e:
        print(f"  ❌ Error loading movimento: {e}")
        return None
    
    try:
        processoassunto_path = get_bronze_path("ejud", "processoassunto")
        print(f"📂 Loading processoassunto from: {processoassunto_path}")
        processoassunto = spark.read.parquet(processoassunto_path)
        print(f"  ✅ Loaded {processoassunto.count():,} records")
        print(f"  📊 Partitions: {processoassunto.rdd.getNumPartitions()}")
        
    except Exception as e:
        print(f"  ❌ Error loading processoassunto: {e}")
        return None
    
    try:
        assunto_path = get_bronze_path("ejud", "assunto")
        print(f"📂 Loading assunto from: {assunto_path}")
        assunto = spark.read.parquet(assunto_path)
        print(f"  ✅ Loaded {assunto.count():,} records")
        
    except Exception as e:
        print(f"  ❌ Error loading assunto: {e}")
        return None
    
    try:
        movjulg_path = get_bronze_path("ejud", "movjulg")
        print(f"📂 Loading movjulg from: {movjulg_path}")
        movjulg = spark.read.parquet(movjulg_path)
        print(f"  ✅ Loaded {movjulg.count():,} records")
        
    except Exception as e:
        print(f"  ❌ Error loading movjulg: {e}")
        return None
    
    try:
        movdespa_path = get_bronze_path("ejud", "movdespa")
        print(f"📂 Loading movdespa from: {movdespa_path}")
        movdespa = spark.read.parquet(movdespa_path)
        print(f"  ✅ Loaded {movdespa.count():,} records")
        
    except Exception as e:
        print(f"  ❌ Error loading movdespa: {e}")
        return None
    
    try:
        tab_judmigracao_path = parquet_path("data/prata/ejud/tab_judmigracao")
        print(f"📂 Loading tab_judmigracao from: {tab_judmigracao_path}")
        tab_judmigracao = spark.read.parquet(tab_judmigracao_path)
        print(f"  ✅ Loaded {tab_judmigracao.count():,} records")
        
    except Exception as e:
        print(f"  ⚠️  Warning: Could not load tab_judmigracao: {e}")
        print(f"     Creating empty DataFrame")
        tab_judmigracao = spark.createDataFrame([], "coddoc: bigint")
    
    print("\n" + "="*80)
    print("STEP 2: Applying Query Filters")
    print("="*80)
    
    # Filter movimento by codfase and dthrmov
    print("\n🔍 Filtering movimento...")
    movimento_filtered = movimento.filter(
        (col("codfase").isin(26, 36)) & 
        (col("dthrmov") < "2025-01-01 00:00:00")
    )
    mov_count = movimento_filtered.count()
    print(f"  After codfase in (26,36) and dthrmov < 2025-01-01: {mov_count:,} records")
    
    # Filter processoassunto by codass (idosos group)
    codass_idosos = [11832, 10951, 3659, 9909, 14692, 14693, 11947]
    print(f"\n🔍 Filtering processoassunto by codass in {codass_idosos}...")
    processoassunto_filtered = processoassunto.filter(col("codass").isin(codass_idosos))
    pa_count = processoassunto_filtered.count()
    print(f"  Processoassunto with these codass: {pa_count:,} records")
    
    # Check specifically for codass 9909
    pa_9909 = processoassunto.filter(col("codass") == 9909)
    print(f"  📌 Processoassunto with codass=9909: {pa_9909.count():,} records")
    
    # Join movimento with processoassunto
    print(f"\n🔗 Joining movimento with processoassunto...")
    result = movimento_filtered.join(
        processoassunto_filtered,
        "coddoc",
        "inner"
    )
    print(f"  After join: {result.count():,} records")
    
    # Join with assunto to get description
    print(f"\n🔗 Joining with assunto...")
    result = result.join(
        assunto.select("codass", col("descr").alias("assunto_descr")),
        "codass",
        "inner"
    )
    print(f"  After join with assunto: {result.count():,} records")
    
    # Prepare movjulg for left anti join (where is null)
    print(f"\n🔗 Preparing movjulg filter (before 2025-01-01)...")
    movjulg_filtered = movjulg.filter(col("dthrmov") < "2025-01-01 00:00:00") \
        .select("coddoc", col("codfase").alias("movjulg_codfase")) \
        .dropDuplicates(["coddoc"])
    print(f"  Movjulg records before 2025: {movjulg_filtered.count():,}")
    
    # Prepare movdespa for left anti join
    print(f"\n🔗 Preparing movdespa filter (before 2025-01-01)...")
    movdespa_filtered = movdespa.filter(col("dthrmov") < "2025-01-01 00:00:00") \
        .select("coddoc", col("codfase").alias("movdespa_codfase")) \
        .dropDuplicates(["coddoc"])
    print(f"  Movdespa records before 2025: {movdespa_filtered.count():,}")
    
    # Prepare movimento with fase_baixa for left anti join
    print(f"\n🔗 Preparing movimento with fase_baixa filter...")
    movimento_baixa = movimento.filter(
        (col("codfase").isin(246, 22, 846, 870, 897, 861)) &
        (col("dthrmov") < "2025-01-01 00:00:00")
    ).select("coddoc", col("codfase").alias("movimento_baixa_codfase")) \
     .dropDuplicates(["coddoc"])
    print(f"  Movimento with fase_baixa: {movimento_baixa.count():,}")
    
    # Apply left anti joins (equivalent to WHERE ... IS NULL)
    print(f"\n🔗 Applying exclusion filters (left anti joins)...")
    result = result.join(movjulg_filtered, "coddoc", "left_anti")
    print(f"  After excluding movjulg: {result.count():,} records")
    
    result = result.join(movdespa_filtered, "coddoc", "left_anti")
    print(f"  After excluding movdespa: {result.count():,} records")
    
    result = result.join(movimento_baixa, "coddoc", "left_anti")
    print(f"  After excluding movimento fase_baixa: {result.count():,} records")
    
    result = result.join(tab_judmigracao.select("coddoc").dropDuplicates(), "coddoc", "left_anti")
    print(f"  After excluding tab_judmigracao: {result.count():,} records")
    
    # Add row_number partitioned by coddoc ordered by dthrmov desc
    print(f"\n📊 Adding row_number...")
    window_spec = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
    result = result.withColumn("rn", row_number().over(window_spec))
    
    # Select final columns
    result = result.select(
        "coddoc", 
        "dthrmov", 
        "codfase", 
        "codass", 
        "assunto_descr",
        "rn"
    ).orderBy("coddoc", "rn")
    
    print("\n" + "="*80)
    print("STEP 3: Final Results from Bronze")
    print("="*80)
    
    total_records = result.count()
    distinct_coddoc = result.select("coddoc").distinct().count()
    
    print(f"\n📊 Total records: {total_records:,}")
    print(f"📊 Distinct coddoc: {distinct_coddoc:,}")
    
    # Show breakdown by codass
    print(f"\n📊 Breakdown by codass:")
    codass_breakdown = result.groupBy("codass", "assunto_descr") \
        .agg(countDistinct("coddoc").alias("distinct_coddoc")) \
        .orderBy("codass")
    codass_breakdown.show(truncate=False)
    
    # Focus on codass 9909
    result_9909 = result.filter(col("codass") == 9909)
    count_9909 = result_9909.count()
    distinct_9909 = result_9909.select("coddoc").distinct().count()
    
    print(f"\n📌 CODASS 9909 ANALYSIS:")
    print(f"  Total records with codass 9909: {count_9909:,}")
    print(f"  Distinct coddoc with codass 9909: {distinct_9909:,}")
    
    if count_9909 > 0:
        print(f"\n  Sample coddoc with codass 9909:")
        result_9909.select("coddoc", "dthrmov", "codfase", "rn") \
            .orderBy("coddoc") \
            .show(20, truncate=False)
    
    return result

def check_filtered_files(spark, result_bronze):
    """Check if filtered files have the expected coddoc values"""
    
    print("\n" + "="*80)
    print("STEP 4: Checking Filtered Files")
    print("="*80)
    
    # Try to find filtered files
    filtered_paths = [
        "/projetos/meta/meta_cnj2025/parquet_files/filtered_coddoc_idosos.parquet"
    ]
    
    filtered_df = None
    used_path = None
    
    for path in filtered_paths:
        try:
            full_path = parquet_path(path)
            print(f"📂 Trying: {full_path}")
            filtered_df = spark.read.parquet(full_path)
            used_path = full_path
            print(f"  ✅ Found filtered file!")
            break
        except Exception as e:
            print(f"  ❌ Not found: {e}")
    
    if filtered_df is None:
        print("\n⚠️  WARNING: Could not find filtered_coddoc_idoso(s).parquet")
        print("   Please check the actual location of this file")
        return
    
    filtered_count = filtered_df.count()
    print(f"\n📊 Filtered file statistics:")
    print(f"  Total records: {filtered_count:,}")
    print(f"  Partitions: {filtered_df.rdd.getNumPartitions()}")
    
    # Show schema
    print(f"\n📋 Schema:")
    filtered_df.printSchema()
    
    # Check if it has codass column
    if "codass" in filtered_df.columns:
        # Check for codass 9909
        filtered_9909 = filtered_df.filter(col("codass") == 9909)
        count_9909 = filtered_9909.count()
        print(f"\n📌 Records with codass 9909 in filtered file: {count_9909:,}")
        
        if count_9909 > 0:
            distinct_coddoc_9909 = filtered_9909.select("coddoc").distinct().count()
            print(f"  Distinct coddoc with codass 9909: {distinct_coddoc_9909:,}")
    
    # Get all coddoc from bronze results
    if result_bronze is not None:
        bronze_coddoc = result_bronze.select("coddoc").distinct()
        bronze_count = bronze_coddoc.count()
        
        # Get coddoc from filtered file
        filtered_coddoc = filtered_df.select("coddoc").distinct()
        filtered_coddoc_count = filtered_coddoc.count()
        
        print(f"\n📊 Comparison:")
        print(f"  Bronze query distinct coddoc: {bronze_count:,}")
        print(f"  Filtered file distinct coddoc: {filtered_coddoc_count:,}")
        
        # Find missing coddoc in filtered file
        missing_in_filtered = bronze_coddoc.join(filtered_coddoc, "coddoc", "left_anti")
        missing_count = missing_in_filtered.count()
        
        print(f"\n🔍 Missing in filtered file: {missing_count:,} coddoc")
        
        if missing_count > 0 and missing_count < 100:
            print(f"\n  Missing coddoc values:")
            missing_in_filtered.show(missing_count, truncate=False)
            
            # Check if these missing coddoc exist in processoassunto
            processoassunto_path = get_bronze_path("ejud", "processoassunto")
            processoassunto = spark.read.parquet(processoassunto_path)
            
            missing_coddoc_list = [row.coddoc for row in missing_in_filtered.collect()]
            pa_with_missing = processoassunto.filter(col("coddoc").isin(missing_coddoc_list))
            
            print(f"\n  Checking processoassunto for these missing coddoc:")
            pa_with_missing.groupBy("coddoc", "codass") \
                .count() \
                .orderBy("coddoc") \
                .show(100, truncate=False)
        
        # Find extra coddoc in filtered file
        extra_in_filtered = filtered_coddoc.join(bronze_coddoc, "coddoc", "left_anti")
        extra_count = extra_in_filtered.count()
        
        print(f"\n🔍 Extra in filtered file (not in bronze query): {extra_count:,} coddoc")
        
        if extra_count > 0 and extra_count < 100:
            print(f"\n  Extra coddoc values:")
            extra_in_filtered.show(extra_count, truncate=False)

def main():
    print("="*80)
    print("DEBUG: Bronze Parquet Verification for CODASS 9909")
    print("="*80)
    print()
    
    spark = create_spark_session()
    
    try:
        # Run query on bronze
        result_bronze = run_query_on_bronze(spark)
        
        # Check filtered files
        check_filtered_files(spark, result_bronze)
        
        print("\n" + "="*80)
        print("✅ Debug Complete")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
