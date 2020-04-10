#!/bin/bash

prefix=$1 # acmdb-lab1

tgt_dir=../submitdb/$prefix/src/java/simpledb/
src_dir=../db/$prefix/src/main/java/simpledb/
cp ${src_dir}/* ${tgt_dir}

tgt_dir=../submitdb/$prefix/test/simpledb/
src_dir=../db/$prefix/src/test/java/simpledb/
cp -r ${src_dir}/* ${tgt_dir}
