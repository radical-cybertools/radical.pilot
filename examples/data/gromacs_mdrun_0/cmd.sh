
mdrun -nt 1 \
      -cpo state.cpt \
      -o traj.trr \
      -e ener.edr \
      -s topol.tpr \
      -g mdlog.log \
      -c outgro
