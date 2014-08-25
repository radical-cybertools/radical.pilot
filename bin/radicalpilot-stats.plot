

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
    pilot_queue_1_dat     = '/tmp/rp.' . session . '.pilot.queue.'     . pilot_1_id . '.dat'
    unit_states_1_dat     = '/tmp/rp.' . session . '.unit.states.'     . pilot_1_id . '.dat'
    unit_callbacks_1_dat  = '/tmp/rp.' . session . '.unit.callbacks.'  . pilot_1_id . '.dat'
  # print 'pilot 1     : ' . pilot_1_name
}
if (pilotnum >= 2) {
    pilot_states_2_dat    = '/tmp/rp.' . session . '.pilot.states.'    . pilot_2_id . '.dat'
    pilot_callbacks_2_dat = '/tmp/rp.' . session . '.pilot.callbacks.' . pilot_2_id . '.dat'
    pilot_slots_2_dat     = '/tmp/rp.' . session . '.pilot.slots.'     . pilot_2_id . '.dat'
    pilot_queue_2_dat     = '/tmp/rp.' . session . '.pilot.queue.'     . pilot_2_id . '.dat'
    unit_states_2_dat     = '/tmp/rp.' . session . '.unit.states.'     . pilot_2_id . '.dat'
    unit_callbacks_2_dat  = '/tmp/rp.' . session . '.unit.callbacks.'  . pilot_2_id . '.dat'
  # print 'pilot 2     : ' . pilot_2_name
}

if (pilotnum >= 3) {
    pilot_states_3_dat    = '/tmp/rp.' . session . '.pilot.states.'    . pilot_3_id . '.dat'
    pilot_callbacks_3_dat = '/tmp/rp.' . session . '.pilot.callbacks.' . pilot_3_id . '.dat'
    pilot_slots_3_dat     = '/tmp/rp.' . session . '.pilot.slots.'     . pilot_3_id . '.dat'
    pilot_queue_3_dat     = '/tmp/rp.' . session . '.pilot.queue.'     . pilot_3_id . '.dat'
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
        term_mult  = 6.0
        term_x     = '6000'
        term_y     = '6000'
        term_font  = 'Monospace,8'
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
    set style line 100 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 101 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 102 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 103 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 104 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 105 lt 2 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 106 lt 1 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*2

    set style line 200 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 201 lt 1 lc rgb '#66AA66' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 202 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 203 lt 1 lc rgb '#66AA66' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 204 lt 1 lc rgb '#99FF44' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 205 lt 2 lc rgb '#66AA66' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 206 lt 1 lc rgb '#66AA66' pt 7 ps term_mult*0.6 lw term_mult*2

    set style line 300 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 301 lt 1 lc rgb '#6666AA' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 302 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*2
    set style line 303 lt 1 lc rgb '#6666AA' pt 6 ps term_mult*0.4 lw term_mult*1
    set style line 304 lt 1 lc rgb '#9944FF' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 305 lt 2 lc rgb '#6666AA' pt 7 ps term_mult*0.6 lw term_mult*3
    set style line 306 lt 1 lc rgb '#6666AA' pt 7 ps term_mult*0.6 lw term_mult*2

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
  # set mxtics mtimetics
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
        plot pilot_states_1_dat    using 1:($2-0.05) title '' with steps  ls 100 , \
             pilot_callbacks_1_dat using 1:($2+0.05) title '' with points ls 101
    }                                                
    if (pilotnum==2) {                               
        plot pilot_states_1_dat    using 1:($2-0.05) title '' with steps  ls 100 , \
             pilot_callbacks_1_dat using 1:($2+0.00) title '' with points ls 101 , \
             pilot_states_2_dat    using 1:($2+0.05) title '' with steps  ls 200 , \
             pilot_callbacks_2_dat using 1:($2+0.10) title '' with points ls 201
    }                                                
    if (pilotnum==3) {                               
        plot pilot_states_1_dat    using 1:($2-0.10) title '' with steps  ls 100 , \
             pilot_callbacks_1_dat using 1:($2-0.05) title '' with points ls 101 , \
             pilot_states_2_dat    using 1:($2-0.00) title '' with steps  ls 200 , \
             pilot_callbacks_2_dat using 1:($2+0.05) title '' with points ls 201 , \
             pilot_states_3_dat    using 1:($2+0.10) title '' with steps  ls 300 , \
             pilot_callbacks_3_dat using 1:($2+0.15) title '' with points ls 301
    }
 
    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set yrange [0:12]
    set ytics  ("NEW            "  1, \
                "STATE_X        "  2, \
                "PEND.   INPUT  "  3, \
                "TRANSF. INPUT  "  4, \
                "PEND. EXECUTION"  5, \
                "SCHEDULING     "  6, \
                "EXECUTING      "  7, \
                "PEND.   OUTPUT "  8, \
                "TRANSF. OUTPUT "  9, \
                "DONE           " 10, \
                "CANCELED       " 11, \
                "FAILED         " 12)

    set xlabel ''
    set ylabel "UNITS\n[states]" offset second -0.06,0
    set format x ""
    set grid

    if (pilotnum==1) {
        plot unit_states_1_dat    using 1:($2-0.05) title '' with steps ls 100 , \
             unit_callbacks_1_dat using 1:($2+0.05) title '' with points ls 101
    }
    if (pilotnum==2) {
        plot unit_states_1_dat    using 1:($2-0.05) title '' with steps  ls 100 , \
             unit_callbacks_1_dat using 1:($2+0.00) title '' with points ls 101 , \
             unit_states_2_dat    using 1:($2+0.05) title '' with steps  ls 200 , \
             unit_callbacks_2_dat using 1:($2+0.10) title '' with points ls 201
    }
    if (pilotnum==3) {
        plot unit_states_1_dat    using 1:($2-0.10) title '' with steps  ls 100 , \
             unit_callbacks_1_dat using 1:($2-0.05) title '' with points ls 101 , \
             unit_states_2_dat    using 1:($2-0.00) title '' with steps  ls 200 , \
             unit_callbacks_2_dat using 1:($2+0.05) title '' with points ls 201 , \
             unit_states_3_dat    using 1:($2+0.10) title '' with steps  ls 300 , \
             unit_callbacks_3_dat using 1:($2+0.15) title '' with points ls 301
    }

    # ------------------------------------------------------------------------------------
    set xrange  [0:maxtime]
    set yrange  [0:slotsscale]
    set mytics  nodesize
    set ytics   autofreq
    set mytics  0
    set y2tics  autofreq
    set y2range [0:queuescale]
    set my2tics 0

    set xlabel ''
    set ylabel "PILOT ACTIVITY\n[slots / queue]" offset second -11,0
    set grid 
  unset format

    if (pilotnum==1) {
      plot pilot_slots_1_dat using 1:($2+0.0) title '' with lines ls 104 , \
           pilot_queue_1_dat using 1:($2+0.0) title '' with steps ls 106 axes x1y2 , \
           slotnum_1                          title '' with lines ls 105 
    }
    if (pilotnum==2) {
      plot pilot_slots_1_dat using 1:($2+0.0) title '' with lines ls 104 , \
           pilot_queue_1_dat using 1:($2+0.0) title '' with steps ls 106 axes x1y2 , \
           (slotnum_1+0.0)                    title '' with lines ls 105 , \
           pilot_slots_2_dat using 1:($2+0.1) title '' with lines ls 204 , \
           pilot_queue_2_dat using 1:($2+0.1) title '' with steps ls 206 axes x1y2 , \
           (slotnum_2+0.1)                    title '' with lines ls 205  
    }
    if (pilotnum==3) {
      plot pilot_slots_1_dat using 1:($2+0.0) title '' with lines ls 104 , \
           pilot_queue_1_dat using 1:($2+0.0) title '' with steps ls 106 axes x1y2 , \
           (slotnum_1+0.0)                    title '' with lines ls 105 , \
           pilot_slots_2_dat using 1:($2+0.1) title '' with lines ls 204 , \
           pilot_queue_2_dat using 1:($2+0.1) title '' with steps ls 206 axes x1y2 , \
           (slotnum_2+0.1)                    title '' with lines ls 205 , \
           pilot_slots_3_dat using 1:($2+0.2) title '' with lines ls 304 , \
           pilot_queue_3_dat using 1:($2+0.2) title '' with steps ls 306 axes x1y2 , \
           (slotnum_3+0.2)                    title '' with lines ls 305 
    }

    # ------------------------------------------------------------------------------------
    # Key plot
    set   tmargin 7
    set   lmargin 24
    set   border  lw 0
    unset tics
    unset xlabel
    set   ylabel ""# "Legend and\nStatistics" offset second -17,0
    set   yrange [0:1]
    if (pilotnum==1) {
      set  key center left reverse
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states changes'                        , \
           NaN ls 101 t 'pilot/unit states notifications'                  , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 105 t 'total number of slots'                            , \
           NaN ls 106 t 'unit queue length'                                , \
           NaN lw   0 t ' '
    }
    if (pilotnum==2) {
      set   key top left reverse maxrows 7
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states changes'                        , \
           NaN ls 101 t 'pilot/unit states notified to application   '     , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 105 t 'total number of slots'                            , \
           NaN ls 106 t 'unit queue length'                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t 'PILOT 2 ('.pilot_2_name.'):'                      , \
           NaN ls 200 t 'pilot/unit states changes'                        , \
           NaN ls 201 t 'pilot/unit states notifications'                  , \
           NaN ls 204 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 205 t 'total number of slots'                            , \
           NaN ls 206 t 'unit queue length'                                , \
           NaN lw   0 t ' '
    }
    if (pilotnum==3) {
      set   key top left reverse maxrows 7
      plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
           NaN ls 100 t 'pilot/unit states changes'                        , \
           NaN ls 101 t 'pilot/unit states notifications'                  , \
           NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 105 t 'total number of slots'                            , \
           NaN ls 106 t 'unit queue length'                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t 'PILOT 2 ('.pilot_2_name.'):'                      , \
           NaN ls 200 t 'pilot/unit states changes'                        , \
           NaN ls 201 t 'pilot/unit states notifications'                  , \
           NaN ls 204 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 205 t 'total number of slots'                            , \
           NaN ls 206 t 'unit queue length'                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t 'PILOT 3 ('.pilot_3_name.'):'                      , \
           NaN ls 300 t 'pilot/unit states changes'                        , \
           NaN ls 301 t 'pilot/unit states notifications'                  , \
           NaN ls 304 t 'busy slot (i.e. used CPU core)'                   , \
           NaN ls 305 t 'total number of slots'                            , \
           NaN ls 306 t 'unit queue length'                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '                                                , \
           NaN lw   0 t ' '
    }
    # ------------------------------------------------------------------------------------

    unset multiplot
    # ------------------------------------------------------------------------------------

}




# ------------------------------------------------------------------------------
# vim: ft=gnuplot

