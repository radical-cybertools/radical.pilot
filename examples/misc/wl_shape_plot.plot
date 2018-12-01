#!/usr/bin/env gnuplot 

set term pdf size 11cm,10cm


# ------------------------------------------------------------------------------
#
set output  "app_stats_tx_heat.pdf"
# set origin  -0.02, 0.0
set cblabel "runtime (s)" 
set cbtics  scale 0
set cbrange [0:200]
set cbtics  25
set palette maxcolors 10
set size    square

#  set title  "Application Performance Map"
set xlabel "# processes"
set ylabel "# threads"
# set logscale z
# set log cb

set xrange [1:]
set yrange [1:]
set xtics 0,2,16
set ytics 0,2,16

set   pm3d map corners2color c2
unset grid
set   dgrid3d 16,16
set   surface
set   view map
unset contour
unset key


# set   cbtics 10, 10, 200
# set   colorbox

splot "./app_map_tx.dat" using 1:2:3 title "T_x [sec]" with pm3d


# ------------------------------------------------------------------------------
#
set term pdf size 20cm,10cm
set output "app_stats_tx_cont.pdf"
set nocbtics
set origin -0.02, 0.0
set cblabel "runtime (s)" 
set cbtics  scale 0
set view map
set dgrid3d
#  set title  "Application Performance Map"
set xlabel "# processes"
set ylabel "# threads"

set xrange [1:]
set yrange [1:]
set xtics 0,2,16 
set ytics 0,2,16 
set pm3d  map

unset surface       # Switch off the surface    
set   view map      # Set a bird eye (xy plane) view    
set   contour       # Plot contour lines    
set   key top right
set   cntrparam levels discrete 10,20,25,30,40,60,80,85,90,100,120
unset colorbox

set cbrange [1:200]  # Set the color range of contour values.
set palette model RGB defined ( 0 'white', 1 'black' )


splot "./app_map.dat" using 1:2:3 title "T_x [sec]" with pm3d


# ------------------------------------------------------------------------------
#
set term pdf size 35cm,5cm
set output "app_cfg_tx_multi.pdf"
set size ratio 0.33
set multiplot layout 1,3 rowsfirst title "Adaptive Application Configuration"
set xrange [0:]
set yrange [0:20]
set key    auto
set xtics  auto
set ytics  auto
set title  ""
set xlabel ""
set xlabel "task id"
set ylabel "\# processes"
plot "< sort -n ./app_stats_tx.dat" using 1:2 title '# processes' with steps
set ylabel "\# threads"
plot "< sort -n ./app_stats_tx.dat" using 1:3 title '# threads'   with steps
set ylabel "runtime T_x [sec]"
set yrange [0:200]
plot "< sort -n ./app_stats_tx.dat" using 1:4 title 'T_x'         with steps
unset multiplot


