#!/bin/bash

prefix=$1 # acmdb-lab1

tgt_dir=../submitdb/$prefix/src/java/simpledb/
src_dir=../db/acmdb-lab/src/main/java/simpledb/
cp ${src_dir}/* ${tgt_dir}
