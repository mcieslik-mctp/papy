#!/usr/bin/env Rscript
library(rjson)
library(stringr)
ARGS = fromJSON(file=commandArgs(TRUE))
PARAMS = ARGS[["params"]]
IN = ARGS[["in"]]
OUT = ARGS[["out"]]
