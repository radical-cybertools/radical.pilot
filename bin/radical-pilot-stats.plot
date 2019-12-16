
# '#FF9944'
# '#AA6666'
# '#FF9944'
# '#AA6666'
# '#FF9944'
# '#AA6666'
# '#AA6666'
#
# '#99FF44'
# '#66AA66'
# '#99FF44'
# '#66AA66'
# '#99FF44'
# '#66AA66'
# '#66AA66'
#
# '#9944FF'
# '#6666AA'
# '#9944FF'
# '#6666AA'
# '#9944FF'
# '#6666AA'
# '#6666AA'

# print 'plot title   : ' . plottitle
# print 'session id   : ' . session
# print 'max time     : ' . maxtime
# print 'timetics     : ' . timetics
# print 'maxslots     : ' . maxslots
# print 'pilotnum     : ' . pilotnum
# print 'nodesize     : ' . nodesize

print 'pilot_num       : ' . pilot_num
print 'pilot_id_list   : ' . pilot_id_list
print 'pilot_name_list : ' . pilot_name_list

pilot_states_dat_list    = ''
pilot_callbacks_dat_list = ''
pilot_slots_dat_list     = ''
pilot_queue_dat_list     = ''
unit_states_dat_list     = ''
unit_callbacks_dat_list  = ''
offset_list              = ''

color_list_1 = "#FF8855 #88FF55 #8855FF #FF55FF #55FFFF"
color_list_2 = "#AA8855 #88AA55 #8855AA #AA55AA #55AAAA"

do for [i=1:pilot_num] {
    pid = word(pilot_id_list, i)

    print ''
    print 'pilot name      : '  . word(pilot_name_list, i)
    print 'pilot id        : '  . pid

    pilot_states_dat         = '/tmp/rp.' . session . '.pilot.states.'    . pid . '.dat'
    pilot_callbacks_dat      = '/tmp/rp.' . session . '.pilot.callbacks.' . pid . '.dat'
    pilot_slots_dat          = '/tmp/rp.' . session . '.pilot.slots.'     . pid . '.dat'
    pilot_queue_dat          = '/tmp/rp.' . session . '.pilot.queue.'     . pid . '.dat'
    unit_states_dat          = '/tmp/rp.' . session . '.unit.states.'     . pid . '.dat'
    unit_callbacks_dat       = '/tmp/rp.' . session . '.unit.callbacks.'  . pid . '.dat'

    pilot_states_dat_list    = pilot_states_dat_list    . pilot_states_dat    . ' '
    pilot_callbacks_dat_list = pilot_callbacks_dat_list . pilot_callbacks_dat . ' '
    pilot_slots_dat_list     = pilot_slots_dat_list     . pilot_slots_dat     . ' '
    pilot_queue_dat_list     = pilot_queue_dat_list     . pilot_queue_dat     . ' '
    unit_states_dat_list     = unit_states_dat_list     . unit_states_dat     . ' '
    unit_callbacks_dat_list  = unit_callbacks_dat_list  . unit_callbacks_dat  . ' '

}

offset(i,n) = (i * n)  -  (pilot_num / 2 * n)
hex(i)      = "0123456789ABCDEF"[i:i]
hexx(i)     = hex(i).hex(i)
color_1(i)  = word(color_list_1,i)
color_2(i)  = word(color_list_2,i)
get_title(i)= sprintf("%s: %-15s (%4s cores / %4s units)", \
                      word(pilot_id_list,i), word(pilot_name_list,i), \
                      word(slotnum_list,i),  word(unitnum_list,i))

# print 'palette '
# do for [i=1:pilot_num] {
#     print  '        ' . color_1(i) . '  ' . color_2(i)
# }
#
# print 'offsets'
# do for [i=1:pilot_num] {
#     print  '        ' . sprintf ("%f", offset (i, 0.2))
#     print word(slotnum_list,i)+offset(i,0.5)
# }
#
# print 'pilot_queue_dats'
# do for [i=1:pilot_num] {
#     print word(pilot_queue_dat_list,i)
# }

do for [term_i=1:words(terms)] {
    term = word(terms, term_i)
    term_t = term.'cairo'

    # --------------------------------------------------------------------------------------------------
    #
    # base parameters
    #
    set key Left left

    if (term eq 'pdf') {
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

  # set mxtics     10
  # set mytics     10
    set tics       scale 1.5

    set term       term_t enhanced color dashed \
        size       term_x,term_y \
        font       term_font     \
        fontscale  term_mult     \
        dashlength term_dl       \
        linewidth  term_lw

    # --------------------------------------------------------------------------------------------------
    set output './'.sname.'.'.term
    print      './'.sname.'.'.term

    set title  ''

    set tmargin  0
    set bmargin  0
    set lmargin 25
    set rmargin 10
    set border  lw 4.0

  unset y2label
  unset y2tics
  unset y2range

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

  # set style line 100 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
  # set style line 101 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.4 lw term_mult*1
    plot for [i=1:pilot_num] \
         word(pilot_states_dat_list,i)    using 1:($2+offset(i,0.05))      title '' with steps  lt 1 lc rgb color_1(i) lw term_mult*2, \
         for [i=1:pilot_num] \
         word(pilot_callbacks_dat_list,i) using 1:($2+offset(i,0.05)+0.05) title '' with points lt 1 lc rgb color_2(i) lw term_mult*1

    # ------------------------------------------------------------------------------------
    set xrange [0:maxtime]
    set yrange [0:12]
    set ytics  ("NEW            "  1, \
                "SCHEDULING     "  2, \
                "PEND.   INPUT  "  3, \
                "TRANSF. INPUT  "  4, \
                "PEND. EXECUTION"  5, \
                "ALLOCATING     "  6, \
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

  # set style line 100 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
  # set style line 101 lt 1 lc rgb '#AA6666' pt 6 ps term_mult*0.4 lw term_mult*1
    plot for [i=1:pilot_num] \
         word(unit_states_dat_list,i)    using 1:($2+offset(i,0.05))      title '' with steps  lt 1 lc rgb color_1(i) lw term_mult*2, \
         for [i=1:pilot_num] \
         word(unit_callbacks_dat_list,i) using 1:($2+offset(i,0.05)+0.05) title '' with points lt 1 lc rgb color_2(i) lw term_mult*1

    # ------------------------------------------------------------------------------------
    set xrange  [0:maxtime]
    set yrange  [0:slotsscale]
    set mytics  nodesize
    set ytics   autofreq nomirror
    set mytics  0
    set y2tics  autofreq
    set y2range [0:queuescale]
    set my2tics 0

    set xlabel  'time (in seconds)'
    set ylabel  "PILOT ACTIVITY\n[slots / queue]" offset second -00,0
    set y2label "UNIT WAIT QUEUE SIZE"            offset second -00.0,0
    set grid
  unset format

  # set style line 104 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*3
  # set style line 106 lt 1 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*2
  # set style line 105 lt 2 lc rgb '#AA6666' pt 7 ps term_mult*0.6 lw term_mult*3
    plot for [i=1:pilot_num] \
         word(pilot_slots_dat_list,i) using 1:($2+offset(i,0.25))     title '' with steps  lt 1 lc rgb color_1(i) lw term_mult*3, \
         for [i=1:pilot_num] \
         word(pilot_queue_dat_list,i) using 1:($2+offset(i,0.1)+0.25) title '' with steps  lt 1 lc rgb color_2(i) lw term_mult*2 axes x1y2 , \
         for [i=1:pilot_num] \
         word(slotnum_list,i)+offset(i,0.2) title '' with lines  lt 2 lc rgb color_1(i) lw term_mult*3

    # ------------------------------------------------------------------------------------
    # Key plot
    set   tmargin 7
    set   lmargin 24
    set   border  lw 0
    unset tics
    unset xlabel
    set   ylabel ""# "Legend and\nStatistics" offset second -17,0
    set   yrange [0:1]
  unset y2label
  unset y2tics
  unset y2range


    set   key top left reverse maxrows 7

  # set style line 100 lt 1 lc rgb '#FF9944' pt 7 ps term_mult*0.6 lw term_mult*2
    plot for [i=1:pilot_num] \
         NaN with lines title get_title(i) lt 1 lc rgb color_1(i) lw term_mult*2

#   plot NaN lw   0 t 'PILOT 1 ('.pilot_1_name.'):'                      , \
#        NaN ls 100 t 'pilot/unit states changes'                        , \
#        NaN ls 101 t 'pilot/unit states notifications'                  , \
#        NaN ls 104 t 'busy slot (i.e. used CPU core)'                   , \
#        NaN ls 105 t 'total number of slots'                            , \
#        NaN ls 106 t 'unit queue length'                                , \
#        NaN lw   0 t ' '
#   }

    unset multiplot
}

# ------------------------------------------------------------------------------
# vim: ft=gnuplot

