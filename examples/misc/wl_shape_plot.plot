#!/usr/bin/env gnuplot 

set term pdf size 20cm,10cm

set output "app_stats.pdf"
set nocbtics
set origin -0.02, 0.0
set cblabel "runtime (s)" 
set cbtics  scale 0
set view map
set dgrid3d
set title  "Application Performance Map"
set xlabel "# Processes"
set ylabel "# Threads"

splot "./app_map.log" using 1:2:3 with pm3d

set yrange [0:7]

# set output "app_cfg.pdf"
# 
# plot "./app_stats.log" using 1:2 title 'Processes' with steps, \
#      "./app_stats.log" using 1:3 title 'Threads'   with steps, \
#      "./app_stats.log" using 1:4 title 'Metric'    with steps



set term pdf size 20cm,20cm
set output "app_cfg_multi.pdf"
set multiplot layout 3,1 rowsfirst title "Adaptive Application Configuration"
set title  ""
set xlabel ""
set ylabel "# procs"
plot "< sort -n ./app_stats.log" using 1:2 title 'processes' with lines
set ylabel "# threads"
plot "< sort -n ./app_stats.log" using 1:3 title 'treads'    with lines
set xlabel "Task ID"
set ylabel "runtime (s)"
plot "< sort -n ./app_stats.log" using 1:4 title 'metric'    with lines
unset multiplot


