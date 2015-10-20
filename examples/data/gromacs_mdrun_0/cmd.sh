
mpirun -n 4 mdrun  \
    -nt 4 \
    -pin on \
    -o traj.trr \
    -e ener.edr \
    -s topol.tpr \
    -g mdlog.log \
    -cpo state.cpt \
    -c outgro
