
print 'session id: ' . session
print 'max time  : ' . maxtime
print 'timetics  : ' . timetics
print 'slotnum   : ' . slotnum
print 'nodesize  : ' . nodesize

events_dat = '/tmp/' . session . '.events.dat'
slots_dat  = '/tmp/' . session . '.slots.dat'

terms = 'png pdf'
do for [term_i=1:words(terms)] {
    t = word(terms, term_i)
    term_t = t.'cairo'

    # --------------------------------------------------------------------------------------------------
    #
    # base parameters
    #
    set key Left left

    if (t eq 'pdf') {
        term_mult  = 6.0
        term_x     = 70
        term_y     = 50
        term_font  = 'Monospace,6'
        term_dl    = 7
        term_lw    = 3

        set key    font ",6"
        set xlabel font ",6"
        set ylabel font ",6"
        set title  font ",6"

    } else {
        term_mult  = 8.0
        term_x     = '6000'
        term_y     = '4000'
        term_font  = 'Monospace,6'
        term_dl    = 6
        term_lw    = 1

        set key    font ",8"
        set xlabel font ",8"
        set ylabel font ",8"
        set title  font ",8"
    }
    
    set style line 1 lt 1 lc rgb '#FF9944' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 2 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 3 lt 1 lc rgb '#FF9944' pt 6 ps term_mult*0.2 lw term_mult*4
    set style line 4 lt 1 lc rgb '#6666AA' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 5 lt 1 lc rgb '#FF4400' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 6 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 7 lt 1 lc rgb '#66AA66' pt 6 ps term_mult*0.6 lw term_mult*1
    set style line 8 lt 1 lc rgb '#6666AA' pt 6 ps term_mult*0.6 lw term_mult*1
    
    set border lw 4.0
  # set mxtics 10
  # set mytics 10
    set tics   scale 1.5

    set term   term_t enhanced color size term_x,term_y font term_font fontscale term_mult dashed dashlength term_dl linewidth term_lw
      
    # --------------------------------------------------------------------------------------------------
    set output './'.session.'.'.t 
    print      './'.session.'.'.t

    set title  ''

    set tmargin 0
    set bmargin 0
    set lmargin 25
    set rmargin 13

  # set size 1.0,1.5
    set multiplot layout 4,1 title ""

    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set xtics  timetics
    set yrange [0:8]
    set ytics  ("PENDING LAUNCH" 1, \
                "LAUNCHING     " 2, \
                "PENDING ACTIVE" 3, \
                "ACTIVE        " 4, \
                "DONE          " 5, \
                "CANCELED      " 6, \
                "FAILED        " 7)
    set xlabel  ''
    set ylabel "PILOTS\n[states]" offset second -0.06,0
    set format x ""
    set grid

    plot \
        '<(grep -e "^pilot" -e "^ *$" '.events_dat.' | grep -e "state" -e "^ *$")' \
        using 3:4 title '' with linespoints ls 1 , \
        '<(grep -e "^pilot" -e "^ *$" '.events_dat.' | grep -e "callback" -e "^ *$")' \
        using 3:4 title '' with linespoints ls 2
 
    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set yrange [0:12]
    set ytics  ("NEW            "  1, \
                "PEND. EXECUTION"  2, \
                "SCHEDULING     "  3, \
                "PEND.   INPUT  "  4, \
                "TRANSF. INPUT  "  5, \
                "EXECUTING      "  6, \
                "PEND.   OUTPUT "  7, \
                "TRANSF. OUTPUT "  8, \
                "DONE           "  9, \
                "CANCELED       " 10, \
                "FAILED         " 11)

    set xlabel ''
    set ylabel "UNITS\n[states]" offset second -0.06,0
    set format x ""
    set grid

    plot \
        '<(grep -e "^unit" -e "^ *$" '.events_dat.' | grep -e "state" -e "^ *$")' \
        using 3:4 title '' with linespoints ls 1 , \
        '<(grep -e "^unit" -e "^ *$" '.events_dat.' | grep -e "callback" -e "^ *$")' \
        using 3:4 title '' with linespoints ls 2



    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set yrange [-1:slotnum+1]
    set ytics  nodesize

    set xlabel 'time'
    set ylabel "SLOTS\n[busy]" offset second -14,0
  unset format
    set grid

    plot \
        '<(grep -e "^pilot" -e "^ *$" '.slots_dat.')' using 3:4 title '' with lines ls 3

    unset multiplot
    # ------------------------------------------------------------------------------------

}




# ------------------------------------------------------------------------------
# vim: ft=gnuplot

