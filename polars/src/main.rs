use glob::glob;
use polars::prelude::*;
use polars::toggle_string_cache;
use std::fs::File;
use std::time::SystemTime;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

type ParquetPath = &'static str;

// Directory of dataset to use.
const MILLION: ParquetPath = "100m-dataset";
const THOUSAND: ParquetPath = "100k-dataset";
const BILLION: ParquetPath = "1b-dataset-parquet";

// Directory containing parquet files.
const COMPNAMES_DIR: ParquetPath = "compnames.parquet";
const CVSS_DIR: ParquetPath = "cvss.parquet";
const FINDINGS_DIR: ParquetPath = "findings.parquet";

// Dataset to use
const DATASET: ParquetPath = MILLION;

fn read_parquet_dir(path: String) -> Result<LazyFrame> {
    // Initialize an empty DataFrame with an empty Series. Each parquet file will generate a new
    // DataFrame that'll be added to this.
    let mut seen = false;
    let v: Vec<Series> = Vec::new();
    let mut main = DataFrame::new(v)?;

    let mut glob_path = path.to_owned();
    glob_path.push_str("/part*");

    println!("{:?}", glob_path);

    for entry in glob(&glob_path).expect("failed to read glob pattern") {
        if let Ok(path) = entry {
            // println!("{:?}", path);
            let f = File::open(path.display().to_string())?;
            let df = ParquetReader::new(f)
                .finish()
                .expect("failed to parse parquet");

            // This is the first dataframe, so replace the existing one in order to use the correct
            // schema.
            if !seen {
                main = df;
                seen = true;
            } else {
                main = main.vstack(&df)?;
            }
            // println!("HEIGHT: {}", main.height());
        }
    }

    println!("Total rows: {}", main.height());

    // Polars is faster with contiguous memory
    main.rechunk();

    // String data copying is expensive
    for i in 0..main.width() {
        main.may_apply_at_idx(i, |s| match s.dtype() {
            DataType::Utf8 => s.cast::<CategoricalType>(),
            _ => Ok(s.clone()),
        });
    }

    Ok(main.lazy())
}

// left join <COMPUTER_NAME>.eid on <CVES>.eid
fn join(compnames: &LazyFrame, cves: &LazyFrame) -> Result<u128> {
    let cloned = cves.clone();
    let compnames_clone = compnames.clone();
    let now = SystemTime::now();
    compnames_clone
        .left_join(cloned, col("eid"), col("eid"), None)
        .collect()?;
    let elapsed = now
        .elapsed()
        .expect("something went wrong with the time thingy");
    Ok(elapsed.as_millis())
}

// multiply cvss score by 10
fn transform(cves: &LazyFrame) -> Result<u128> {
    let cloned = cves.clone();
    let now = SystemTime::now();

    cloned
        .map(
            |df: DataFrame| -> Result<DataFrame> {
                let mut copy = df;
                copy.apply("cvss", |series| series * 10)?;
                Ok(copy)
            },
            None,
            None,
        )
        .collect()?;

    let elapsed = now
        .elapsed()
        .expect("something went wrong with the time thingy");
    Ok(elapsed.as_millis())
}

// group CVEs by eid, then sum all cvss scores
fn groupby_agg(cves: &LazyFrame) -> Result<u128> {
    let cloned = cves.clone();
    let now = SystemTime::now();

    cloned
        .groupby(vec![col("eid")])
        .agg(vec![col("cvss").sum()])
        .collect()?;

    let elapsed = now
        .elapsed()
        .expect("something went wrong with the time thingy");
    Ok(elapsed.as_millis())
}

// remove all rows from CVE where cvss < 5
fn filter(cves: &LazyFrame) -> Result<u128> {
    let cloned = cves.clone();
    let now = SystemTime::now();

    cloned.filter(col("cvss").lt(lit(0.5))).collect()?;

    let elapsed = now
        .elapsed()
        .expect("something went wrong with the time thingy");
    Ok(elapsed.as_millis())
}

// Comply - CVE Findings
fn cve_findings(lf: &LazyFrame) -> Result<LazyFrame> {
    let cloned = lf.clone();
    let result = cloned
        .groupby(vec![col("eid")])
        .agg(vec![col("cvss").count()]);
    Ok(result)
}

// Comply - Compliance Findings
fn compliance_findings(lf: &LazyFrame) -> Result<LazyFrame> {
    // Separate lazyframe containing only rows with State != "pass", grouped-by "eid",
    // and then counted.
    let fail_lf = lf
        .clone()
        .filter(col("state").neq(lit("pass")))
        .groupby(vec![col("eid")])
        .agg(vec![col("eid").count()])
        .with_column_renamed("eid_count", "eid_fail_count");

    let result = lf
        .clone()
        .groupby(vec![col("eid")])
        .agg(vec![col("eid").count()])
        .inner_join(fail_lf, col("eid"), col("eid"), None)
        .map(
            |df: DataFrame| -> Result<DataFrame> {
                let total_col = df
                    .column("eid_count")?
                    .cast_with_dtype(&DataType::Float64)?;

                let fail_col = df
                    .column("eid_fail_count")?
                    .cast_with_dtype(&DataType::Float64)?;

                // calc eid_count / eid_fail_count and add as col named "pct_failed"
                let mut pct_failed = &fail_col / &total_col;
                pct_failed.rename("pct_failed");

                // add new pct_failed col to the df
                df.hstack(&[pct_failed])
            },
            None,
            None,
        );

    Ok(result)
}

fn main() -> Result<()> {
    toggle_string_cache(true);
    let create_path =
        |dataset: ParquetPath, dir: ParquetPath| format!("{}/{}/{}", get_data_root(), dataset, dir);

    let compnames = read_parquet_dir(create_path(DATASET, COMPNAMES_DIR))?;
    let cves = read_parquet_dir(create_path(DATASET, CVSS_DIR))?;

    println!("transform (ms): {}", transform(&cves)?);
    println!("join (ms): {}", join(&compnames, &cves)?);
    println!("groupby_agg (ms): {}", groupby_agg(&cves)?);
    println!("filter (ms): {}", filter(&cves)?);

    Ok(())
}

fn get_data_root() -> String {
    std::env::var("DATA_ROOT").unwrap()
}
