#!/usr/bin/env python3

from pathlib import Path

from colmena.models import ExecutableTask


class Simulation(ExecutableTask):

    def __init__(self, executable: Path):
        super().__init__(executable=[executable.absolute()],
                         mpi=True,
                         mpi_command_string='')

    def preprocess(self, run_dir, args, kwargs):
        return [str(args[0])], None

    def postprocess(self, run_dir: Path):
        with open(run_dir / 'colmena.stdout') as fp:
            return float(fp.read().strip())
