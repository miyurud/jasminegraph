#!/bin/bash
file_name_pre="../JesminGraph"
current_time=$(date "+%Y-%m-%d-%H-%M-%S")
file_name_post=".tar.gz"
file_name=$file_name_pre'-'$current_time$file_name_post
echo $file_name
tar -cvf $file_name "../jesmingraph"