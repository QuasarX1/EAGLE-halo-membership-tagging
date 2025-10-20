# SPDX-FileCopyrightText: 2025-present Christopher Rowe <chris.rowe19@outlook.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import errno
import os
import socket
import sys
from typing import Callable

from astro_sph_tools import ParticleType
import dask
import dask.array as da
from dask.distributed import LocalCluster
from matplotlib import pyplot as plt
import xarray as xr
from xarray.core import dtypes as xr_dtypes
import dask.array as dask_array
from dask.utils import SerializableLock
import numpy as np
import h5py as h5
from QuasarCode import Console, Settings, Stopwatch
from QuasarCode.IO.Configurations import ConfigsBase, YamlConfig

from eagle_tag import EAGLE_Files, EAGLE_Snapshot, SnapshotTag, TagSequence, load_snapshot, load_catalogue, load_hdf5_files_with_xarray, make_aux_file_path as make_complete_membership_file_path, load_catalogue_membership
from ._calculate_reorder import make_file_path as make_reorder_file_path
from ._run_trace import TraceSettings



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL



def main() -> None:
    print(
"""
--|| EAGLE-tag trace (1D) ||--

Plot a tracked particle quantity over time.

Default time axis is redshift.

Output data is stored in a zarr store in the following format:
    <outputs>.zarr/
        PartType<i>/
            ParticleIDs  -> (Coordinate) The ID of each requested particle.
            Redshifts    -> (Coordinate) The redshift of each selected snapshot.
            <field-name> -> [ParticleIDs, Redshifts, *<remaining-data-dimensions>]
""")

    Console.show_times()
    Console.reset_stopwatch()

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|
    #region Arguments
    Console.print_info("Parsing command line arguments.")

    parser = argparse.ArgumentParser(description = "Select and cache particle data.")

    parser.add_argument("--id",                        type = int,                                  help = "Target particle ID.")
    parser.add_argument("--dark-matter",               action  = "store_true",                      help = "Target is a dark matter particle.")
    parser.add_argument("--gas",                       action  = "store_true",                      help = "Target is a gas particle.")
    parser.add_argument("--star",                      action  = "store_true",                      help = "Target is a star particle.")
    parser.add_argument("--black-hole",                action  = "store_true",                      help = "Target is a black hole particle.")
    parser.add_argument("--field",                     type = str,                                  help = "Target field to plot.")
    parser.add_argument("--default",                   type = float, default = None,                help = "Value to replace NaN data with.")
    parser.add_argument("--expansion",                 action  = "store_true",                      help = "Plot expansion factor on the X-axis.")
    parser.add_argument("--file",                      type = str, default = None,                  help = "Save the plot to this file. When not specified, interactive window will be used.")
    parser.add_argument("--settings",                  type = str, default = "trace-settings.yaml", help = "File containing the settings for the run.")
    parser.add_argument("--output-directory",   "-o",  type = str, default = ".",                   help = "Directory in which to find the output file. Default is the current working directory.")
    parser.add_argument("--verbose",            "-v",  action  = "store_true",                      help = "Display extra information.")
    parser.add_argument("--debug",              "-d",  action  = "store_true",                      help = "Display extreme amounts of information.")

    # This will exit the program if -h or --help are specified
    args = parser.parse_args()

    if args.verbose:
        Settings.enable_verbose()
    if args.debug:
        Settings.enable_verbose()
        Settings.enable_debug()

    Console.print_verbose_info("Arguments:")
    for key in args.__dict__:
        Console.print_verbose_info(f"    {key}: {getattr(args, key)}")

    if not (args.gas or args.dark_matter or args.star or args.black_hole):
        Console.print_error("At least one particle type must be specified.")
        sys.exit(1)

    allow_gas_to_star = args.gas and args.star

    total_specified_parttypes = int(args.gas) + int(args.dark_matter) + int(args.star) + int(args.black_hole)
    if total_specified_parttypes > 2 or total_specified_parttypes == 2 and not allow_gas_to_star:
        Console.print_error("Only one particle type can be specified, except for gas and star which can be specified together.")
        sys.exit(1)

    #endregion Arguments

    #---------------|
    # Load settings |
    #---------------|

    settings = TraceSettings.from_file(args.settings)

    #-----------|
    # Load data |
    #-----------|

    gas_data:  xr.Dataset
    dm_data:   xr.Dataset
    star_data: xr.Dataset
    bh_data:   xr.Dataset

    data: xr.Dataset

    if args.gas:
        try:
            gas_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.gas.common_hdf5_name)
        except KeyError:
            Console.print_error("No traced gas particle data.")
            sys.exit(1)

        if args.id not in gas_data["ParticleIDs"]:
            Console.print_error(f"Gas particle ID {args.id} not found in traced data.")
            sys.exit(1)

        if not allow_gas_to_star:
            data = gas_data.sel(ParticleIDs = args.id, drop = True)
        else:
            gas_data = gas_data.sel(ParticleIDs = args.id, drop = True)

    if args.dark_matter:
        try:
            dm_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.dark_matter.common_hdf5_name)
        except KeyError:
            Console.print_error("No traced dark matter particle data.")
            sys.exit(1)

        if args.id not in dm_data["ParticleIDs"]:
            Console.print_error(f"Dark matter particle ID {args.id} not found in traced data.")
            sys.exit(1)

        data = dm_data.sel(ParticleIDs = args.id, drop = True)

    if args.star:
        try:
            star_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.star.common_hdf5_name)
        except KeyError:
            Console.print_error("No traced star particle data.")
            sys.exit(1)

        if args.id not in star_data["ParticleIDs"]:
            Console.print_error(f"Star particle ID {args.id} not found in traced data.")
            sys.exit(1)

        if not allow_gas_to_star:
            data = star_data.sel(ParticleIDs = args.id, drop = True)
        else:
            star_data = star_data.sel(ParticleIDs = args.id, drop = True)

    if args.black_hole:
        try:
            bh_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.black_hole.common_hdf5_name)
        except KeyError:
            Console.print_error("No traced black hole particle data.")
            sys.exit(1)

        if args.id not in bh_data["ParticleIDs"]:
            Console.print_error(f"Black hole particle ID {args.id} not found in traced data.")
            sys.exit(1)

        data = bh_data.sel(ParticleIDs = args.id, drop = True)

    # Its reasonable to wish to track a gas particle after it turned into a star
    if allow_gas_to_star:
        pass#TODO: find where the particle disappears from one dataset and appears in the other (SnapshotParticleIndex != NULL_INDEX)
    #TODO: consolidate to create data object - does xr.where work over a whole Dataset???

    #data = data.isel(Redshifts = slice(0,160))

    x_label: str
    x_data: xr.DataArray
    if not args.expansion:
        x_label = "z"
        x_data  = data["Redshifts"]
        #x_data  = np.arange(len(data["Redshifts"]))
    else:
        x_label = "a"
        x_data  = 1 / (data["Redshifts"] + 1)

    y_label = args.field
    y_data  = data[args.field]

    if args.default is not None:
        y_data = y_data.fillna(args.default)

    plt.plot(x_data, y_data)
    plt.ylabel(y_label)
    plt.xlabel(x_label)
    plt.xlim(x_data[0], x_data[-1])

    if args.file is not None:
        plt.savefig(args.file)
    else:
        plt.show()
