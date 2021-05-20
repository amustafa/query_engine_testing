# Disclaimer: This is not beautiful code.

using DataFrames
using CSV
using Profile

wd = pwd()
println(wd)

DATA_ROOT = ENV["DATA_ROOT"]
println("DATA ROOT", DATA_ROOT)


DATASET_DIR = joinpath(DATA_ROOT, "100m-dataset/csvs")
# DATASET_DIR = joinpath(DATA_ROOT, "1b-dataset-parquet/csvs")

findingsFile = joinpath(DATASET_DIR, "findings.csv");
compNameFile = joinpath(DATASET_DIR, "compnames.csv");
cvssFile = joinpath(DATASET_DIR, "cvss.csv");

# Everything below here is a bit messy / was adjusted for running benchmarks ... at the expense of
# readability.

# Run it `numIters` times (+1 to get JAOT compilation; ignore first run).
numIters = 2

function main(mode)
  println("running in ", mode, " mode")

  if "leftjoin" == mode
    for _ = 1:numIters + 1
      findings  = DataFrame(CSV.File(findingsFile))
      nameData = DataFrame(CSV.File(compNameFile))

      @time leftjoin(nameData, findings, on = :eid)

      # Free the RAMs!
      findingsData = nothing
      nameData = nothing
      GC.gc(true)
    end
    println("Done.")
  elseif "transform" == mode
    for _ = 1:numIters + 1
      cvssData = DataFrame(CSV.File(cvssFile))
      @time DataFrame(
        eid = cvssData.eid,
        cvss = cvssData.cvss * 10
      )
      cvssData = nothing
      GC.gc(true)
    end
    println("Done.")
  elseif "transform-in-place" == mode
    println("Loading data ...")
    for _ = 1:numIters + 1
      cvssData = DataFrame(CSV.File(cvssFile))
      @time cvssData.cvss *= 10
      f = nothing
      GC.gc(true)
    end
    println("Done.")
    # Free the RAMs!
    cvssData = nothing
  elseif "filter" == mode
    println("Loading data ...")
    cvssData = DataFrame(CSV.File(cvssFile))
    println("Done.")
    println("Benchmarking ...")
    for _ = 1:numIters + 1
      @time filter(:cvss => x -> x > 0.5, cvssData)
      GC.gc(true)
    end
    println("Done.")
    # Free the RAMs!
    cvssData = nothing
  elseif "filter2" == mode
    for _ = 1:numIters + 1
      cvssData = DataFrame(CSV.File(cvssFile))
      @time filter(:cvss => x -> x > 0.5, cvssData)
      cvssData = nothing
      GC.gc(true)
    end
    println("Done.")
  elseif "group-aggregate" == mode
    for _ = 1:numIters + 1
        cvssData = DataFrame(CSV.File(cvssFile))
      @time combine(
        groupby(cvssData, :eid),
        :cvss => (d -> 5 * sum(d)) => :scoresSum
      )
      cvssData = nothing
      GC.gc(true)
    end
    println("Done.")
  end
  GC.gc(true)
end

# Select which benchmark(s) to run here!
main("leftjoin")
main("transform")
main("transform-in-place")
main("filter")
main("filter2")
main("group-aggregate")
