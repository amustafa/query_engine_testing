use datafusion::datasource::{parquet::ParquetTable, MemTable};
use datafusion::prelude::*;
use datafusion::logical_plan::JoinType;
use std::sync::Arc;
use std::time::Instant;


type ParquetPath = &'static str;

// Directory of dataset to use.
const MILLION: ParquetPath = "100m-dataset";
const THOUSAND: ParquetPath = "100k-dataset";
const BILLION: ParquetPath = "1b-dataset-parquet";

// Directory containing parquet files.
const COMPNAMES_DIR: ParquetPath = "compnames.parquet";
const CVSS_DIR: ParquetPath = "cvss.parquet";

// Dataset to use
const DATASET: ParquetPath = MILLION;

const BATCH_SIZE: usize = 65536;
// const BATCH_SIZE: usize = usize::pow(2, 19);
// const BATCH_SIZE: usize = 0;

const NUM_CPUS: usize = 12;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let cve_findings_memtable = Arc::new(read_parquet_to_memtable(CVSS_DIR, BATCH_SIZE).await);
    let computer_names_memtable = Arc::new(read_parquet_to_memtable(COMPNAMES_DIR, BATCH_SIZE).await);

    let new_ctx = ||  {
        if BATCH_SIZE > 0 {
            ExecutionContext::with_config(ExecutionConfig::new().with_batch_size(BATCH_SIZE))
        } else {
            ExecutionContext::with_config(ExecutionConfig::new())
        }
    };

    //===================Left Join Benchmark=================
    let mut ctx = new_ctx();

    ctx.register_table("computer_names", computer_names_memtable.clone())?;
    ctx.register_table("cve_findings", cve_findings_memtable.clone())?;

    let computer_names_df = ctx.table("computer_names")?.clone();
    let cve_findings_df = ctx.table("cve_findings")?.clone();

    let now = Instant::now();
    let cves_and_computer_names = computer_names_df.join(
        cve_findings_df,
        JoinType::Left,
        &["eid"], &["eid"])?
      .select(vec![col("eid"), col("compname"), col("cvss")])?;
    let _result = cves_and_computer_names.collect().await?;
    println!("Execute Time for Left Join2: {:.2?}", now.elapsed());


    //===================Aggregate Benchmark=================
    let mut ctx = new_ctx();

    ctx.register_table("cve_findings", cve_findings_memtable.clone())?;
    let cve_findings_df = ctx.table("cve_findings")?;

    let now = Instant::now();
    let cvss_summed_per_eid_df = cve_findings_df.aggregate(vec![col("eid")],
                                                           vec![sum(col("cvss").alias("Sum of CVSS scores"))])?;
    let _result = cvss_summed_per_eid_df.collect().await?;
    println!("Execute Time for Aggregate2: {:.2?}", now.elapsed());


    //===================Transform Benchmark===================
    let mut ctx = new_ctx();
    ctx.register_table("cve_findings", cve_findings_memtable.clone())?;
    let cve_findings_df = ctx.table("cve_findings")?;

    let now = Instant::now();
    let cvss_times_ten_df = cve_findings_df.select(vec![(col("cvss") * lit(10)).alias("cvss times ten")])?;
    let _result = cvss_times_ten_df.collect().await?;
    println!("Execute Time for Transform: {:.2?}", now.elapsed());

    //==================Filtration Benchmark====================
    let mut ctx = new_ctx();
    ctx.register_table("cve_findings", cve_findings_memtable.clone())?;
    let cve_findings_df = ctx.table("cve_findings")?;

    let now = Instant::now();
    let cvss_lt_p5 = cve_findings_df.filter(col("cvss").lt(lit(0.5)))?;
    let _result = cvss_lt_p5.collect().await?;
    println!("Execute Time for Filtration: {:.2?}", now.elapsed());

    Ok(())
}


async fn read_parquet_to_memtable(data_dir: &str, batch_size: usize) -> MemTable
{
    let mut path: String = get_data_root();
    path.push_str("/");
    path.push_str(DATASET);
    path.push_str("/");
    path.push_str(data_dir);

    println!("{}", path);

    let max_concurrency = NUM_CPUS;
    let parquet_read = ParquetTable::try_new(&path, max_concurrency).unwrap();
    MemTable::load(Arc::new(parquet_read), batch_size, Some(max_concurrency)).await.unwrap()
}

fn get_data_root() -> String {
    std::env::var("DATA_ROOT").unwrap()
}
