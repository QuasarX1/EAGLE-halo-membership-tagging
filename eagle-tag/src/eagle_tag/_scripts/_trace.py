# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import sys

from QuasarCode import Console

from ._run_trace import main as run_trace
from ._show_trace import main as show_trace
from ._plot_position import main as run_plot_position
from ._plot_1d import main as run_plot_property



commandlets = {
    "particles" : run_trace,
    "show"      : show_trace,
    "position"  : run_plot_position,
    "1D"        : run_plot_property,
}



def main():
    arguments = list(sys.argv[1:])

    if len(arguments) == 0 or arguments[0].replace("-", "").lower() in ("help", "h"):
        # Print command information and exit
        print(
"""
--|| EAGLE-tag trace ||--

Tools for tracing particle properties across snapshots.

usage: eagle-tag trace [commandlet] [options | --help]
       eagle-tag [--help | -h]

Commandlets:

    particles   ->  Track particle properties and store the results.

    show        ->  Display a string representation of traced data.

    position    ->  Plot the positions of traced particles in 2D.

    1D          ->  Plot the change in a single particle quantity.
""",
            flush = True
        )
        sys.exit(0)
        
    else:
        # Look for a script to run

        if arguments[0] in commandlets:
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            Console.show_times()
            Console.reset_stopwatch()
            commandlets[arguments[0]]()
            sys.exit(0) # If the commandlet returns, assume success

        else:
            Console.print_error(f"EAGLE-tag trace: unrecognised commandlet: {arguments[0]}", flush=True)
            sys.exit(1)



if __name__ == "__main__":
    main()
