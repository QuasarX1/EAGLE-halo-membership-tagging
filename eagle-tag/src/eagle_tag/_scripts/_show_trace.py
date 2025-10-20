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



def main() -> None:
    print(
"""
--|| EAGLE-tag trace (show) ||--

Display the data from a particle trace.

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

    #endregion Arguments

    #---------------|
    # Load settings |
    #---------------|

    settings = TraceSettings.from_file(args.settings)

    #-----------|
    # Load data |
    #-----------|

    Console.print_raw()

    try:
        gas_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.gas.common_hdf5_name)
        Console.print_info(f"Gas Particles:\n{gas_data}")
    except KeyError:
        Console.print_verbose_info("No traced gas particle data.")

    Console.print_raw()

    try:
        dm_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.dark_matter.common_hdf5_name)
        Console.print_info(f"Dark Matter Particles:\n{dm_data}")
    except KeyError:
        Console.print_verbose_info("No traced dark matter particle data.")

    Console.print_raw()

    try:
        star_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.star.common_hdf5_name)
        Console.print_info(f"Star Particles:\n{star_data}")
    except KeyError:
        Console.print_verbose_info("No traced star particle data.")

    Console.print_raw()

    try:
        bh_data = xr.open_zarr(os.path.join(args.output_directory, f"{settings.outputs}.zarr"), group = ParticleType.black_hole.common_hdf5_name)
        Console.print_info(f"Black Hole Particles:\n{bh_data}")
    except KeyError:
        Console.print_verbose_info("No traced black hole particle data.")
