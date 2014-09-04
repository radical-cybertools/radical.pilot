[merzky@s1 pilot-53ec9a9b20a64164121d7d65]$ grep --color='auto' -P -n "[\x80-\xFF]" u*/*OUT
unit-53ec9a9e20a64164121d7d68/STDOUT:107:    src/mpi4py.MPI.c: In function ��initMPI��:
unit-53ec9a9e20a64164121d7d68/STDOUT:108:    src/mpi4py.MPI.c:107349: warning: ��ompi_mpi_ub�� is deprecated (declared at /opt/openmpi-1.8.1/include/mpi.h:910)
unit-53ec9a9e20a64164121d7d68/STDOUT:109:    src/mpi4py.MPI.c:107364: warning: ��ompi_mpi_lb�� is deprecated (declared at /opt/openmpi-1.8.1/include/mpi.h:909)

