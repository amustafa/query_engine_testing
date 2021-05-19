# Julia / `DataFrames` Evaluation

## Running This

Here are (probably-incomplete) instructions for running this code on your Mac (They also work on
Linux, except the `brew` command; I tested on Manjaro.):

- install Julia:
  - `brew install julia`
- install packages:
  - start Julia
  - mash `]` (enters package manager)
  - `add DataFrames`
  - `add CSV`
  - mash `{backspace}` (exits package manager)
  - `exit()` (quits interpreter)
- edit `RiskCalc.jl`:
  - set `inputPath` to wherever your CSVs are
  - (optional) comment out one/more of the `main("{test}")` tests
- run it:
  - `time -l julia benchmark.jl`
