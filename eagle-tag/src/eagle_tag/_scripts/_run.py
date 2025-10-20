# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import sys

from QuasarCode import Console

from ._translate_membership import main as run_translate_membership
from ._calculate_reorder import main as run_calculate_reorder
from ._track_structure import main as run_track_structure
from ._trace import main as particle_trace



commandlets = {
    "membership": run_translate_membership,
    "reorder"   : run_calculate_reorder,
    "track"     : run_track_structure,
    "trace"     : particle_trace,
}



def main():
    arguments = list(sys.argv[1:])

    if len(arguments) == 0 or arguments[0].replace("-", "").lower() in ("help", "h"):
        # Print command information and exit
        print(
"""
--|| EAGLE-tag ||--

Suite of software for manipulating EAGLE catalogue data.

usage: eagle-tag [commandlet] [options | --help]
       eagle-tag [--help | -h]

Commandlets:

    help        ->  Displays this message.

    membership  ->  Generates an auxiliary file tagging all particles with their current group and
                        subhalo membership.

    reorder     ->  Computes the reorder indexes for moving between particle orders.

    track       ->  Tags particles with the properties of the structure of which they were last a
                        member.

    trace       ->  Traces the properties of specific particles and their structure properties
                        across snapshots.
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
            Console.print_error(f"EAGLE-tag: unrecognised commandlet: {arguments[0]}", flush=True)
            sys.exit(1)



if __name__ == "__main__":
    main()
