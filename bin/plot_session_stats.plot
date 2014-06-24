

# print 'plot title   : ' . plottitle
# print 'session id   : ' . session
# print 'max time     : ' . maxtime
# print 'timetics     : ' . timetics
# print 'maxslots     : ' . maxslots
# print 'pilotnum     : ' . pilotnum
# print 'nodesize     : ' . nodesize


if (pilotnum >= 1) {
    pilot_states_1_dat    = '/tmp/rp.' . session . '.pilot.states.'    . pilot_1_id . '.dat'
    pilot_callbacks_1_dat = '/tmp/rp.' . session . '.pilot.callbacks.' . pilot_1_id . '.dat'
    pilot_slots_1_dat     = '/tmp/rp.' . session . '.pilot.slots.'     . pilot_1_id . '.dat'
    unit_states_1_dat     = '/tmp/rp.' . session . '.unit.states.'     . pilot_1_id . '.dat'
    unit_callbacks_1_dat  = '/tmp/rp.' . session . '.unit.callbacks.'  . pilot_1_id . '.dat'
  # print 'pilot 1     : ' . pilot_1_name
}
if (pilotnum >= 2) {
    pilot_states_2_dat    = '/tmp/rp.' . session . '.pilot.states.'    . pilot_2_id . '.dat'
    pilot_callbacks_2_dat = '/tmp/rp.' . session . '.pilot.callbacks.' . pilot_2_id . '.dat'
    pilot_slots_2_dat     = '/tmp/rp.' . session . '.pilot.slots.'     . pilot_2_id . '.dat'
    unit_states_2_dat     = '/tmp/rp.' . session . '.unit.states.'     . pilot_2_id . '.dat'
    unit_callbacks_2_dat  = '/tmp/rp.' . session . '.unit.callbacks.'  . pilot_2_id . '.dat'
  # print 'pilot 2     : ' . pilot_2_name
}

if (pilotnum >= 3) {
    pilot_states_3_dat    = '/tmp/rp.' . session . '.pilot.states.'    . pilot_3_id . '.dat'
    pilot_callbacks_3_dat = '/tmp/rp.' . session . '.pilot.callbacks.' . pilot_3_id . '.dat'
    pilot_slots_3_dat     = '/tmp/rp.' . session . '.pilot.slots.'     . pilot_3_id . '.dat'
    unit_states_3_dat     = '/tmp/rp.' . session . '.unit.states.'     . pilot_3_id . '.dat'
    unit_callbacks_3_dat  = '/tmp/rp.' . session . '.unit.callbacks.'  . pilot_3_id . '.dat'
  # print 'pilot 3     : ' . pilot_3_name
}

if (pilotnum >= 4) {
    print "only support up to three pilots"
    exit
}


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
        term_y     = 70
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

    # pilot 1: pilot states, pilot notifications
    #          unit states, unit state notifications, 
    #          slots, maxslots  
    set style line 100 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 101 lt 1 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 102 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 103 lt 1 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 104 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 105 lt 2 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*2

    set style line 200 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 201 lt 1 lc rgb '#66AA66' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 202 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 203 lt 1 lc rgb '#66AA66' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 204 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 205 lt 2 lc rgb '#66AA66' pt 7 ps term_mult*0.6 lw term_mult*2

    set style line 300 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 301 lt 1 lc rgb '#6666AA' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 302 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 303 lt 1 lc rgb '#6666AA' pt 7 ps term_mult*0.6 lw term_mult*1
    set style line 304 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 305 lt 2 lc rgb '#6666AA' pt 7 ps term_mult*0.6 lw term_mult*2

  # set mxtics 10
  # set mytics 10
    set tics   scale 1.5

    set term       term_t enhanced color dashed \
        size       term_x,term_y \
        font       term_font     \
        fontscale  term_mult     \
        dashlength term_dl       \
        linewidth term_lw
      
    # --------------------------------------------------------------------------------------------------
    set output './'.session.'.'.t 
    print      './'.session.'.'.t

    set title  ''

    set tmargin  0
    set bmargin  0
    set lmargin 25
    set rmargin 10
    set border  lw 4.0

  # set size 1.0,1.5
    set multiplot layout 4,1 title "\n\n" . plottitle . "\n\n\n\n"

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

    if (pilotnum==1) {
        plot pilot_states_1_dat    title '' with linespoints ls 100 , \
             pilot_callbacks_1_dat title '' with linespoints ls 101
    }
    if (pilotnum==2) {
        plot pilot_states_1_dat    title '' with linespoints ls 100 , \
             pilot_callbacks_1_dat title '' with linespoints ls 101 , \
             pilot_states_2_dat    title '' with linespoints ls 200 , \
             pilot_callbacks_2_dat title '' with linespoints ls 201
    }
    if (pilotnum==3) {
        plot pilot_states_1_dat    title '' with linespoints ls 100 , \
             pilot_callbacks_1_dat title '' with linespoints ls 101 , \
             pilot_states_2_dat    title '' with linespoints ls 200 , \
             pilot_callbacks_2_dat title '' with linespoints ls 201 , \
             pilot_states_3_dat    title '' with linespoints ls 300 , \
             pilot_callbacks_3_dat title '' with linespoints ls 301
    }
 
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

    if (pilotnum==1) {
        plot unit_states_1_dat    title '' with linespoints ls 102 , \
             unit_callbacks_1_dat title '' with linespoints ls 103
    }
    if (pilotnum==2) {
        plot unit_states_1_dat    title '' with linespoints ls 102 , \
             unit_callbacks_1_dat title '' with linespoints ls 103 , \
             unit_states_2_dat    title '' with linespoints ls 202 , \
             unit_callbacks_2_dat title '' with linespoints ls 203
    }
    if (pilotnum==3) {
        plot unit_states_1_dat    title '' with linespoints ls 102 , \
             unit_callbacks_1_dat title '' with linespoints ls 103 , \
             unit_states_2_dat    title '' with linespoints ls 202 , \
             unit_callbacks_2_dat title '' with linespoints ls 203 , \
             unit_states_3_dat    title '' with linespoints ls 302 , \
             unit_callbacks_3_dat title '' with linespoints ls 303
    }

    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set yrange [1:maxslots+nodesize/2]
    set ytics  nodesize

    set xlabel 'time'
    set ylabel "SLOTS\n[busy]" offset second -14,0
  unset format
    set grid

    if (pilotnum==1) {
      plot pilot_slots_1_dat using 1:($2+1.0) title '' with lines ls 104 , \
           slotnum_1                          title '' with lines ls 105
    }
    if (pilotnum==2) {
      plot pilot_slots_1_dat using 1:($2-0.2) title '' with lines ls 104 , \
           slotnum_1                          title '' with lines ls 105 , \
           pilot_slots_2_dat using 1:($2+0.2) title '' with lines ls 204 , \
           slotnum_2                          title '' with lines ls 205
    }
    if (pilotnum==3) {
      plot pilot_slots_1_dat using 1:($2-0.3) title '' with lines ls 104 , \
           slotnum_1                          title '' with lines ls 105 , \
           pilot_slots_2_dat using 1:($2+0.0) title '' with lines ls 204 , \
           slotnum_2                          title '' with lines ls 205 , \
           pilot_slots_3_dat using 1:($2+0.3) title '' with lines ls 304 , \
           slotnum_3                          title '' with lines ls 305
    }

    # ------------------------------------------------------------------------------------
    # Key plot
    set   tmargin 3
    set   lmargin 24
    set   border  lw 0
    unset tics
    unset xlabel
    set   ylabel "Legend and\nStatistics" offset second -17,0
    set   yrange [0:1]
    if (pilotnum==1) {
      set   key top left reverse
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 101 t 'pilot/unit states notified to application'        , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t 'STATISTICS:'                                      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " " 
    }
    if (pilotnum==2) {
      set   key top left reverse
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 101 t 'pilot/unit states notified to application'        , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t 'PILOT 2 ('.pilot_2_name.'):'                      , \
           NaN ls 200 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 201 t 'pilot/unit states notified to application'        , \
           NaN ls 204 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t 'STATISTICS:'                                      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " " 
    }

    if (pilotnum==3) {
      set   key top left reverse 
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 101 t 'pilot/unit states notified to application'        , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t 'PILOT 2 ('.pilot_2_name.'):'                      , \
           NaN ls 200 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 201 t 'pilot/unit states notified to application'        , \
           NaN ls 204 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t 'PILOT 3 ('.pilot_3_name.'):'                      , \
           NaN ls 300 t 'pilot/unit states recorded by RP agent'           , \
           NaN ls 301 t 'pilot/unit states notified to application'        , \
           NaN ls 304 t 'busy slot (i.e. used CPU core)'                   , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t 'STATISTICS:'                                      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t "2.25s : mean time Pilot SUBMITTED -> ACTIVE\n"    , \
           NaN lw   0 t "1.23s : mean time CU    SUBMITTED -> EXECUTING\n" , \
           NaN lw   0 t "1.23s : mean time CU    EXECUTING -> DONE\n"      , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                                , \
           NaN lw   0 t " "                                               
    }
    # ------------------------------------------------------------------------------------

    unset multiplot
    # ------------------------------------------------------------------------------------

}




# ------------------------------------------------------------------------------
# vim: ft=gnuplot

