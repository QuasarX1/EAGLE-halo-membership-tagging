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



NULL_INDEX = 2**30 # Used where an integer index needs to be NULL



class TraceSettings(YamlConfig):

    def __new__(cls, filepath: str, *args, **kwargs):
        return cls.from_file(filepath, *args, **kwargs)

    @staticmethod
    def create(filepath: str) -> None:
        content = """\
# EAGLE-tag Particle Tracing Configuration

# Data Sources

eagle_data_directory:                  "./"                   # Root directory containing snapshots and catalogue information.
halo_membership_by_particle_directory: null                   # Set this to enable fast lookup of associated structure properties.
reorder_indexes_directory:             null                   # Set this to enable fast lookup of particle indexes beyond the first file.
target_tags:                           "./targets.txt"        # This should be a list of tags in the format "012_z345p678<newline>" - one per line
outputs:                               "traced-particle-data" # Name of the zarr archive to write to (without file extension).

# Control Options

start_tag:         "000_z020p000" # First snapshot.
end_tag:           "028_z000p000" # Last snapshot.
skip_tags:         []             # Any tags that should be skipped (due to corruption, etc.).
snipshots:         false          # Target snipshots?
include_so_region: false          # Negative GroupNumber values indicate Spherical Overdensity but not FOF membership. Include them?

# Dask Options

dask_workers:                    null # Number of parallel workers to use, 0 will avoid instantiation of a dask cluster
dask_memory_per_worker:          null # in GB
dask_port:                       8787 # Port number or null to disable the dask dashboard

# Target Particle IDs

target_particles:

    dark_matter: [                            # List of particle IDs
    ]
    #dark_matter: "trace-dark-matter-ids.txt" # Alternatively, provide a file containing a list of particle IDs - one per line

    gas: [                                    # List of particle IDs
    ]
    #gas: "trace-gas-ids.txt"                 # Alternatively, provide a file containing a list of particle IDs - one per line

    stars: [                                  # List of particle IDs
    ]
    #stars: "trace-star-ids.txt"              # Alternatively, provide a file containing a list of particle IDs - one per line

    black_holes: [                            # List of particle IDs
    ]
    #black_holes: "trace-black-hole-ids.txt"  # Alternatively, provide a file containing a list of particle IDs - one per line

# Tracked Snapshot Quantities

# The following fields are automatically added for all particle types:
#     ParticleIDs
#     SnapshotParticleIndex
#     GroupNumber
#     SubGroupNumber
#     SubGroupIndex

# Format:
#    <output-field-name>:
#        target:        "/target/field/hdf5/path"
#        target_shape:  null OR "box_axis_index" OR "particle_type_number" OR <custom-length-integer> # This is the shape AFTER the first/particle dimension
#        indexes:       [list,of,column,indexes] OR null
#        default_value: null
#        dark_matter:   true
#        gas:           true
#        stars:         true
#        black_holes:   true

snapshot_quantities:

    Coordinates:
        target:        "/Coordinates"
        target_shape:  "box_axis_index"
        indexes:       [0,1,2]
        default_value: null
        dark_matter:   true
        gas:           true
        stars:         true
        black_holes:   true

    Mass:
        target:        "/Mass"
        target_shape:  null
        indexes:       null
        default_value: null
        dark_matter:   false
        gas:           true
        stars:         true
        black_holes:   false

# Tracked FOF Group Quantities

fof_quantities:

    M200:
        target:        "/Group_M_Crit200"
        target_shape:  null
        indexes:       null
        default_value: null
        dark_matter:   true
        gas:           true
        stars:         true
        black_holes:   true

# Tracked Subhalo Quantities

subhalo_quantities:

    SubhaloGasMass:
        target:        "/MassType"
        target_shape:  "particle_type_number"
        indexes:       [0]
        default_value: null
        dark_matter:   true
        gas:           true
        stars:         true
        black_holes:   true

    SubhaloStellarMass:
        target:        "/MassType"
        target_shape:  "particle_type_number"
        indexes:       [4]
        default_value: null
        dark_matter:   true
        gas:           true
        stars:         true
        black_holes:   true
"""
        with open(filepath, "w") as file:
            file.write(content)



def main() -> None:
    print(
"""
--|| EAGLE-tag trace (particles) ||--

Track particle properties and store the results.

Output data is stored in a zarr store in the following format:
    <outputs>.zarr/
        PartType<i>/
            ParticleIDs  -> (Coordinate) The ID of each requested particle.
            Redshifts    -> (Coordinate) The redshift of each selected snapshot.
            <field-name> -> [ParticleIDs, Redshifts, *<remaining-data-dimensions>]
""")

    Console.show_times()
    Console.reset_stopwatch()

    #region Configuration

    #------------------------------|
    # Parse command line arguments |
    #------------------------------|
    #region Arguments
    Console.print_info("Parsing command line arguments.")

    parser = argparse.ArgumentParser(description = "Select and cache particle data.")

    parser.add_argument("--settings",                  type = str, default = "trace-settings.yaml", help = "File containing the settings for the run.")
    parser.add_argument("--output-directory",   "-o",  type = str, default = ".",                   help = "Directory in which to create the output file. Default is the current working directory.")
    parser.add_argument("--new-settings-file",         action  = "store_true",                      help = "Create a new settings file and exit.")
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

    #------------------------------------------|
    # Check for requesting a new settings file |
    #------------------------------------------|

    if args.new_settings_file:
        Console.print_verbose_info("Creating new settings file.")

        if os.path.exists(args.settings):
            Console.print_error(f"Settings file \"{args.settings}\" already exists. Please move/delete it before creating a new one.\nAlternatively, change the target file by specifying --settings")
            sys.exit(1)

        Console.print_info(f"Creating new settings file \"{args.settings}\".")
        TraceSettings.create(args.settings)

        return

    #----------------------------------|
    # Check for a valid set of options |
    #----------------------------------|
    #region Argument Validation
    Console.print_verbose_info("Validating arguments.")

    if not os.path.exists(args.output_directory):
        Console.print_error(f"Target output directory does not exist (\"{args.output_directory}\").")
        sys.exit(1)

    if not os.path.exists(args.settings):
        Console.print_error(f"Unable to locate settings file at \"{args.settings}\". Create a new one using --new-settings-file")
        sys.exit(1)

    #endregion Argument Validation

    #-----------------------------------|
    # Check for a valid set of settings |
    #-----------------------------------|
    #region Settings Validation
    Console.print_info("Counting particles and validating settings.")

    settings = TraceSettings(args.settings)

    if not os.path.exists(settings.eagle_data_directory):
        Console.print_error(f"Unable to locate EAGLE data at \"{settings.eagle_data_directory}\".")
        sys.exit(1)

    if settings.halo_membership_by_particle_directory is not None and not os.path.exists(settings.halo_membership_by_particle_directory):
        Console.print_error(f"Unable to locate EAGLE-tag structure membership data at \"{settings.halo_membership_by_particle_directory}\".")
        sys.exit(1)

    if settings.reorder_indexes_directory is not None and not os.path.exists(settings.reorder_indexes_directory):
        Console.print_error(f"Unable to locate EAGLE-tag particle reordering data at \"{settings.reorder_indexes_directory}\".")
        sys.exit(1)

    if not os.path.exists(settings.target_tags):
        Console.print_error(f"Unable to locate list of target tags at \"{settings.target_tags}\".")
        sys.exit(1)

    target_particle_ids: dict[ParticleType, list[int]] = {}

    if isinstance(settings.target_particles.gas, str):
        if not os.path.exists(settings.target_particles.gas):
            Console.print_error(f"Unable to locate list of target gas particle IDs at \"{settings.target_particles.gas}\".")
            sys.exit(1)
        else:
            with open(settings.target_particles.gas, "r") as file:
                target_particle_ids[ParticleType.gas] = [ int(line.strip()) for line in file if line.strip() != "" and not line.strip().startswith("#") ]
    else:
        target_particle_ids[ParticleType.gas] = settings.target_particles.gas if settings.target_particles.gas is not None else []

    if isinstance(settings.target_particles.dark_matter, str):
        if not os.path.exists(settings.target_particles.dark_matter):
            Console.print_error(f"Unable to locate list of target gas particle IDs at \"{settings.target_particles.dark_matter}\".")
            sys.exit(1)
        else:
            with open(settings.target_particles.dark_matter, "r") as file:
                target_particle_ids[ParticleType.dark_matter] = [ int(line.strip()) for line in file if line.strip() != "" and not line.strip().startswith("#") ]
    else:
        target_particle_ids[ParticleType.dark_matter] = settings.target_particles.dark_matter if settings.target_particles.dark_matter is not None else []

    if isinstance(settings.target_particles.stars, str):
        if not os.path.exists(settings.target_particles.stars):
            Console.print_error(f"Unable to locate list of target star particle IDs at \"{settings.target_particles.stars}\".")
            sys.exit(1)
        else:
            with open(settings.target_particles.stars, "r") as file:
                target_particle_ids[ParticleType.star] = [ int(line.strip()) for line in file if line.strip() != "" and not line.strip().startswith("#") ]
    else:
        target_particle_ids[ParticleType.star] = settings.target_particles.stars if settings.target_particles.stars is not None else []

    if isinstance(settings.target_particles.black_holes, str):
        if not os.path.exists(settings.target_particles.black_holes):
            Console.print_error(f"Unable to locate list of target black hole particle IDs at \"{settings.target_particles.black_holes}\".")
            sys.exit(1)
        else:
            with open(settings.target_particles.black_holes, "r") as file:
                target_particle_ids[ParticleType.black_hole] = [ int(line.strip()) for line in file if line.strip() != "" and not line.strip().startswith("#") ]
    else:
        target_particle_ids[ParticleType.black_hole] = settings.target_particles.black_holes if settings.target_particles.black_holes is not None else []


    if len(target_particle_ids[ParticleType.gas]) == 0 and len(target_particle_ids[ParticleType.dark_matter]) == 0 and len(target_particle_ids[ParticleType.star]) == 0 and len(target_particle_ids[ParticleType.black_hole]) == 0:
        Console.print_error("No target particles specified. Please provide at least one particle ID in the settings file.")
        sys.exit(1)

    if int(settings.start_tag.split("_")[0]) > int(settings.end_tag.split("_")[0]):
        Console.print_error("Start tag must be earlier than or equal to end tag.")
        sys.exit(1)

    if settings.start_tag in settings.skip_tags:
        Console.print_error("Start tag must not be in the list of skipped tags.")
        sys.exit(1)

    if settings.end_tag in settings.skip_tags:
        Console.print_error("End tag must not be in the list of skipped tags.")
        sys.exit(1)

    if settings.dask_workers < 0:
        Console.print_error("Number of dask workers must be at least 0 (and ideally not 1).")
        sys.exit(1)
    elif settings.dask_workers == 1:
        Console.print_warning("Number of dask workers is set to 1. This will work, but it is preferred to set the value to 0 and avoid using a dask cluster altogether.")

    if settings.dask_workers > 0 and (settings.dask_memory_per_worker is None or settings.dask_memory_per_worker <= 0):
        Console.print_error(f"Invalid amount of memory when using dask cluster. The memory per worker must be a positive, nonzero value (current value was \"{settings.dask_memory_per_worker}\").")
        sys.exit(1)

    if settings.dask_workers > 0 and settings.dask_port is not None and (settings.dask_port < 1 or settings.dask_port > 65535):
        Console.print_error(f"Invalid port for dask client. Provide either \"null\" or a value between 1 and 65535 (current value was \"{settings.dask_port}\").")
        sys.exit(1)

    #endregion Settings Validation

    #endregion Configuration

    #region Input Data Parsing

    #-----------------------------|
    # Construct field information |
    #-----------------------------|
    #region Field Information
    Console.print_verbose_info("Splitting data fields into particle type groups.")

    field_names:     dict[str, dict[ParticleType, list[str           ]]] = {}
    source_fields:   dict[str, dict[ParticleType, list[str           ]]] = {}
    data_indexes:    dict[str, dict[ParticleType, list[list[int]|None]]] = {}
    default_values:  dict[str, dict[ParticleType, list[float         ]]] = {}
    remaining_shape: dict[str, dict[ParticleType, list[str|float|None]]] = {}

    # In cases where target HDF5 data is not in the root of the target group, it needs to be loaded separately by xarray
    nested_source_fields_are_present:   dict[str, dict[ParticleType, bool                     ]] = {}
    nested_source_fields_by_group:      dict[str, dict[ParticleType, dict[str|None, list[str]]]] = {}
    nested_source_original_field_paths: dict[str, dict[ParticleType, dict[str|None, list[str]]]] = {}

    for type_name,   tree_node in {
        "snapshot" : settings.snapshot_quantities,
        "fof"      : settings.fof_quantities,
        "subhalo"  : settings.subhalo_quantities
    }.items():

        field_names    [type_name] = { part_type_string : [] for part_type_string in ParticleType.get_all() }
        source_fields  [type_name] = { part_type_string : [] for part_type_string in ParticleType.get_all() }
        data_indexes   [type_name] = { part_type_string : [] for part_type_string in ParticleType.get_all() }
        default_values [type_name] = { part_type_string : [] for part_type_string in ParticleType.get_all() }
        remaining_shape[type_name] = { part_type_string : [] for part_type_string in ParticleType.get_all() }

        nested_source_fields_are_present  [type_name] = { part_type_string : False for part_type_string in ParticleType.get_all() }
        nested_source_fields_by_group     [type_name] = { part_type_string : {}    for part_type_string in ParticleType.get_all() }
        nested_source_original_field_paths[type_name] = { part_type_string : {}    for part_type_string in ParticleType.get_all() }

        for field_name in tree_node.keys:

            field_info = tree_node[field_name]

            for particle_type,             particle_type_applies in {
                ParticleType.gas         : field_info.gas,
                ParticleType.dark_matter : field_info.dark_matter,
                ParticleType.star        : field_info.stars,
                ParticleType.black_hole  : field_info.black_holes
            }.items():
                
                if particle_type_applies:

                    if field_info.indexes is not None and field_info.target_shape is None:
                        Console.print_error(f"Field \"{field_name}\" has indexes specified but no target shape. This is not valid.")
                        sys.exit(1)

                    field_names    [type_name][particle_type].append(field_name)
                    source_fields  [type_name][particle_type].append(field_info.target.strip().strip("/"))
                    data_indexes   [type_name][particle_type].append(field_info.indexes if field_info.indexes is not None and len(field_info.indexes) > 0 else None)
                    default_values [type_name][particle_type].append(field_info.default_value if field_info.default_value is not None else np.nan)
                    remaining_shape[type_name][particle_type].append(field_info.target_shape)

                    if '/' not in field_info.target.strip().strip("/"):
                        # Data is in the root of the target group
                        nested_source_fields_by_group     [type_name][particle_type][None] = field_info.target.strip().strip("/")
                        nested_source_original_field_paths[type_name][particle_type][None] = field_info.target.strip().strip("/")

                    else:
                        # Data is nested
                        group, target_field_name = field_info.target.strip().strip("/").rsplit("/", 1)
                        nested_source_fields_are_present  [type_name][particle_type] = True # Set this to ensure this edge case is handled later
                        nested_source_fields_by_group     [type_name][particle_type][group] = target_field_name
                        nested_source_original_field_paths[type_name][particle_type][group] = field_info.target.strip().strip("/")

    if any(nested_source_fields_are_present["snapshot"].values()):
        Console.print_error("Nested snapshot fields are not CURRENTLY supported.")#TODO: support this!
        sys.exit(1)

    if any(nested_source_fields_are_present["fof"].values()):
        Console.print_error("Nested FOF fields are not supported. EAGLE data contains no nested FOF datasets - double check the catalogue file path and data!")
        sys.exit(1)

    #endregion Field Information

    #-------------------|
    # Locate EAGLE data |
    #-------------------|
    Console.print_info("Finding EAGLE data.")

    eagle_files = EAGLE_Files(directory = settings.eagle_data_directory)

    #----------------------------|
    # Load and parse target tags |
    #----------------------------|
    #region Load EAGLE

    snapshot_tags = TagSequence.from_file(settings.target_tags)
    snapshot_tags.skip_tags(*settings.skip_tags)
    snapshot_tags.start = settings.start_tag
    snapshot_tags.end   = settings.end_tag

    #endregion Load EAGLE

    #---------------------------------------|
    # Check if membership data is available |
    #---------------------------------------|

    try_use_membership_cache: bool = settings.halo_membership_by_particle_directory is not None
    if try_use_membership_cache:
        Console.print_info("Structure membership data location provided.\nThis will be used where available.")

    #------------------------------------|
    # Check if reorder data is available |
    #------------------------------------|

    try_use_reorder_cache: bool = settings.reorder_indexes_directory is not None
    if try_use_reorder_cache:
        Console.print_info("Particle reorder data location provided.\nThis will be used where available.")

    #endregion Input Data Parsing

    #--------------------|
    # Start dask cluster |
    #--------------------|
    #region Dask

    if settings.dask_workers > 0:
        Console.print_info("Starting dask cluster.", flush = True)

        cluster = LocalCluster(
            n_workers = settings.dask_workers,
            memory_limit = f"{settings.dask_memory_per_worker}GB",
            dashboard_address = f":{settings.dask_port}" if settings.dask_port is not None else None
        )
        client = cluster.get_client()

        Console.print_info(f"Dask cluster running with {settings.dask_workers} workers each allocated {settings.dask_memory_per_worker} GB of memory.")

        if settings.dask_port is not None:
            Console.print_info(f"Dask dashboard available at {socket.gethostname()}:{settings.dask_port}")
        else:
            Console.print_verbose_info("No dask dashboard (dask_port was set to null).")

    #endregion Dask

    #--------------------------------------------------|
    # Configure zarr store and xarray Dataset creation |
    #--------------------------------------------------|
    #region Zarr Configuration

    output_target = os.path.join(args.output_directory, f"{settings.outputs}.zarr")

    Console.print_info(f"Data will be written to: {output_target}")

    if os.path.exists(output_target):
        Console.print_error(f"Output target \"{output_target}\" already exists. Please move/delete it before running or specify a different name.")
        sys.exit(1)

    Console.print_info(f"Number of particles to be traced:\nDark Matter -> {len(target_particle_ids[ParticleType.dark_matter])}\n        Gas -> {len(target_particle_ids[ParticleType.gas])}\n      Stars -> {len(target_particle_ids[ParticleType.star])}\nBlack Holes -> {len(target_particle_ids[ParticleType.black_hole])}")

    def create_blank_write_targets(particle_type: ParticleType) -> None:

        if os.path.exists(output_target):
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), output_target)

        redshift_list = np.empty(shape = snapshot_tags.count_selected, dtype = np.float64)
        Console.print_debug("Reading file redshifts:")
        for i, tag in enumerate(snapshot_tags.selected):
            Console.print_debug(f"    {tag}")
            with h5.File(eagle_files.snapshot(tag, settings.snipshots).snapshot_file_template.format(0), "r") as file:
                redshift_list[i] = file["Header"].attrs["Redshift"]
        Console.print_debug("    done.")

        number_of_particles = len(target_particle_ids[particle_type])
        number_of_redshifts = snapshot_tags.count_selected
        dummy_data_shape =  xr.Dataset(
            attrs = {
            },
            coords = {
                "ParticleIDs" : xr.DataArray(name = "ParticleIDs", data = target_particle_ids[particle_type], dims = ("ParticleIDs",)).astype(np.uint64),
                "Redshifts"   : xr.DataArray(name = "Redshifts",   data = redshift_list,                      dims = ("Redshifts",)  ).astype(np.float64),
            },
            data_vars = {
                "SnapshotParticleIndex" : xr.DataArray(data = da.empty(shape = (number_of_particles, number_of_redshifts)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint64),
                "GroupNumber"           : xr.DataArray(data = da.empty(shape = (number_of_particles, number_of_redshifts)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),
                "SubGroupNumber"        : xr.DataArray(data = da.empty(shape = (number_of_particles, number_of_redshifts)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),
                "SubGroupIndex"         : xr.DataArray(data = da.empty(shape = (number_of_particles, number_of_redshifts)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, number_of_redshifts, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension ]if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["snapshot"][particle_type], data_indexes["snapshot"][particle_type], remaining_shape["snapshot"][particle_type])
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, number_of_redshifts, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension ]if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["fof"][particle_type],      data_indexes["fof"][particle_type],      remaining_shape["fof"][particle_type])
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, number_of_redshifts, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension ]if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["subhalo"][particle_type],  data_indexes["subhalo"][particle_type],  remaining_shape["subhalo"][particle_type])
            }
        )

        Console.print_info(f"Output data template:\n{dummy_data_shape}")

        dummy_data_shape.to_zarr(
            output_target, mode = "w",
            group = particle_type.common_hdf5_name,
            compute = False
        )

    def create_empty_file_dataset(particle_type: ParticleType, redshift: float) -> xr.Dataset:
        number_of_particles = len(target_particle_ids[particle_type])
        return xr.Dataset(
            attrs = {
            },
            coords = {
                "ParticleIDs" : xr.DataArray(name = "ParticleIDs", data = target_particle_ids[particle_type], dims = ("ParticleIDs",)).astype(np.uint64),
                "Redshifts"   : xr.DataArray(name = "Redshifts",   data = [redshift],                         dims = ("Redshifts",)  ).astype(np.float64),
            },
            data_vars = {
                "SnapshotParticleIndex" : xr.DataArray(data = np.empty(shape = (number_of_particles, 1)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint64),
                "GroupNumber"           : xr.DataArray(data = np.empty(shape = (number_of_particles, 1)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),#TODO: THESE NEED TO BE SIGNED!!!!!!!!!!!!!!!!!!! 
                "SubGroupNumber"        : xr.DataArray(data = np.empty(shape = (number_of_particles, 1)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),
                "SubGroupIndex"         : xr.DataArray(data = np.empty(shape = (number_of_particles, 1)), dims = ["ParticleIDs", "Redshifts"]).astype(np.uint32),
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, 1, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension] if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["snapshot"][particle_type], data_indexes["snapshot"][particle_type], remaining_shape["snapshot"][particle_type])
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, 1, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension] if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["fof"][particle_type],      data_indexes["fof"][particle_type],      remaining_shape["fof"][particle_type])
            } | {
                name : xr.DataArray(data = np.empty(shape = (number_of_particles, 1, *([len(indexes)] if indexes is not None and len(indexes) > 1 else [] if shape_after_first_dimension is None or len(shape_after_first_dimension) == 0 or (indexes is not None and len(indexes) == 1) else [3] if shape_after_first_dimension == "box_axis_index" else [6] if shape_after_first_dimension == "particle_type_number" else [shape_after_first_dimension]))), dims = ["ParticleIDs", "Redshifts"] + ([] if shape_after_first_dimension is None or (indexes is not None and len(indexes) == 1) else [shape_after_first_dimension] if isinstance(shape_after_first_dimension, str) else [f"unlabled_data_dimension_length_{shape_after_first_dimension}"]))
                for    name,                                   indexes,                                 shape_after_first_dimension
                in zip(field_names["subhalo"][particle_type],  data_indexes["subhalo"][particle_type],  remaining_shape["subhalo"][particle_type])
            }
        )

    def write_data(tag: SnapshotTag, particle_type: ParticleType, data: xr.Dataset) -> None:

        if not os.path.exists(output_target):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), output_target)

        Console.print_info("Writing data.")

        file_index = snapshot_tags.selected.index(tag)
        Console.print_debug(f"File index: {file_index}")

        data.to_zarr(
            output_target, mode = "a",
            group = particle_type.common_hdf5_name,
            region = {
                "ParticleIDs" : slice(0, len(target_particle_ids[particle_type])),
                "Redshifts"   : slice(file_index, file_index + 1),
            }
        )

    #endregion Zarr Configuration

    #------------------------------|
    # Loop over each particle_type |
    #------------------------------|

    for particle_type in ParticleType.get_all():

        if len(target_particle_ids[particle_type]) == 0:
            Console.print_info(f"No target particles specified for {particle_type.name}, skipping.")
            continue

        Console.print_info(f"Processing {particle_type.name}.")

        #----------------------------|
        # Create template zarr store |
        #----------------------------|

        create_blank_write_targets(particle_type)

        #--------------------------------------------------------------------------------|
        # Create somewhere to store the last snapshot's indexes when using reorder files |
        #--------------------------------------------------------------------------------|

        if try_use_reorder_cache:
            last_tag: SnapshotTag
            #last_snapshot_indexes = xr.DataArray(data = np.zeros(shape = (len(target_particle_ids[particle_type]),))).astype(np.int64)
            last_snapshot_indexes: np.ndarray[tuple[int], np.dtype[np.int64]] = np.zeros(shape = (len(target_particle_ids[particle_type]),), dtype = np.int64)

        #----------------|
        # Loop over tags |
        #----------------|

        for tag in snapshot_tags:

            Console.print_info(f"Doing sn{'i' if settings.snipshots else 'a'}pshot {tag}:")

            #-----------------|
            # Load EAGLE info |
            #-----------------|
            #region EAGLE Snapshot Info

            target_eagle_files = eagle_files.snapshot(tag, settings.snipshots)

            with h5.File(target_eagle_files.snapshot_file_template.format(0), "r") as file:
                redshift = float(file["Header"].attrs["Redshift"])

            #endregion EAGLE Snapshot Info

            #-------------------|
            # Create data store |
            #-------------------|

            recorded_data = create_empty_file_dataset(particle_type, redshift)

            #--------------------|
            # Load snapshot data |
            #--------------------|
            #region Load Snapshot
            Console.print_info("Loading EAGLE snapshot data.")

            snapshot_particle_data = load_snapshot(
                target_eagle_files,
                gas_fields         = (source_fields["snapshot"][particle_type] + ["particleIDs"]) if particle_type == ParticleType.gas         else None,
                dark_matter_fields = (source_fields["snapshot"][particle_type] + ["particleIDs"]) if particle_type == ParticleType.dark_matter else None,
                star_fields        = (source_fields["snapshot"][particle_type] + ["particleIDs"]) if particle_type == ParticleType.star        else None,
                black_hole_fields  = (source_fields["snapshot"][particle_type] + ["particleIDs"]) if particle_type == ParticleType.black_hole  else None,
            )[particle_type.common_hdf5_name]
            assert snapshot_particle_data is not None

            #endregion Load Snapshot

            #------------------------------|
            # Locate particles in snapshot |
            #------------------------------|
            #region Find Particles

            snapshot_particle_indexes: np.ndarray[tuple[int], np.dtype[np.int64]]
            available_particles:       np.ndarray[tuple[int], np.dtype[np.bool_]]

            loaded_from_reorder_cache: bool = False
            if tag != snapshot_tags.start and try_use_reorder_cache:
                Console.print_info("Attempting particle index retrieval from reorder information.")

                reorder_data_filepath = make_reorder_file_path(settings.reorder_indexes_directory, last_tag, tag)
                if os.path.exists(reorder_data_filepath):
                    try:

                        reorder_data = load_hdf5_files_with_xarray(
                            [reorder_data_filepath],
                            particle_type.common_hdf5_name,
                            ["ForwardsIndexes"],
                            dimension_sizes = { "snapshot_particle_index" : None }
                        )

                        Console.print_debug(np.where(last_snapshot_indexes >= reorder_data.sizes["snapshot_particle_index"]))
                        Console.print_debug(last_snapshot_indexes[last_snapshot_indexes >= reorder_data.sizes["snapshot_particle_index"]])
                        snapshot_particle_indexes = reorder_data["ForwardsIndexes"].isel(snapshot_particle_index = last_snapshot_indexes).where(available_particles, NULL_INDEX).values
                        loaded_from_reorder_cache = True

                    except Exception as e:
                        Console.print_error("    Failed.")
                        if Settings.debug:
                            raise e
                        else:
                            Console.print_info("    Falling back to direct lookup.")
                            loaded_from_reorder_cache = False

                else:
                    Console.print_error(f"Unable to locate reorder data between {last_tag} and {tag}. Falling back to direct lookup.")

            if not loaded_from_reorder_cache:

                Console.print_info("Locating particles in snapshot data.")

                # Find which of the available particles are being traced
                identified_particle_indexes_in_snapshot = np.where(snapshot_particle_data["ParticleIDs"].isin(recorded_data["ParticleIDs"]))[0]
                # Grab the IDs of those particles, as they are in the wrong order!
                located_particle_ids = snapshot_particle_data["ParticleIDs"].isel(snapshot_particle_index = identified_particle_indexes_in_snapshot)

                # Particle info is in the wrong order, so make a mapping
                # NOTE: this will be slow, but is implemented this way as the number of particles being traced is expected to be small
                #TODO: find a better way of doing this lookup! (possibly, sort, searchsorted & ==)
                located_particle_ids__numpy = np.array(located_particle_ids)
                # Filled with uninitialised memory as values will be re-read only from the selected places - this is ok as a temp approach but would be better replaced with a memory efficient approach in case of many traced particles
                snapshot_particle_indexes = np.empty(shape = len(recorded_data["ParticleIDs"]), dtype = np.uint64)
                for snapshot_index, id in zip(identified_particle_indexes_in_snapshot, located_particle_ids__numpy):
                    snapshot_particle_indexes[np.where(recorded_data["ParticleIDs"] == id)[0][0]] = snapshot_index

                # Get the index into the snapshot for each traced particle, or the NULL_INDEX if it is not present
                snapshot_particle_indexes = xr.where(recorded_data["ParticleIDs"].isin(located_particle_ids), snapshot_particle_indexes, NULL_INDEX).values

            recorded_data["SnapshotParticleIndex"][:, 0] = snapshot_particle_indexes

            # Create a mask that indicates which particles are actually present in this snapshot
            available_particles = np.where(snapshot_particle_indexes != NULL_INDEX, True, False)

            #endregion Find Particles

            #--------------------|
            # Grab snapshot data |
            #--------------------|
            #region Read Snapshot
            Console.print_info("Selecting particle data from snapshot.")

            # This is a delayed isel operation to allow for correct evaluation using xarray.where later on
            selected_snapshot_particle_data = snapshot_particle_data.isel(snapshot_particle_index = da.array(snapshot_particle_indexes)).rename({ "snapshot_particle_index" : "ParticleIDs" })

            Console.print_info("Copying data.")

            for name, field, indexes, default_value in zip(field_names["snapshot"][particle_type], source_fields["snapshot"][particle_type], data_indexes["snapshot"][particle_type], default_values["snapshot"][particle_type]):
                Console.print_debug(f"{name} <- {field}{f"[{",".join(map(str, indexes))}]" if indexes is not None else ""}")
                source = selected_snapshot_particle_data[field]
                if indexes is None:
                    recorded_data[name][:, 0] = source.where(xr.DataArray(available_particles, dims = ["ParticleIDs"]), default_value).values
                else:
                    target_indexes_only = source.isel({source.dims[1] : indexes})
                    if len(indexes) == 1:
                        target_indexes_only = target_indexes_only.squeeze(axis = 1)
                    recorded_data[name][:, 0] = target_indexes_only.where(xr.DataArray(available_particles, dims = ["ParticleIDs"]), default_value).values

            #endregion Read Snapshot

            #------------------------|
            # Grab catalogue indexes |
            #------------------------|
            #region Catalogue Indexes

            fof_data_available: bool
            subhalo_data_available: bool

            loaded_from_membership_cache: bool = False
            if try_use_membership_cache:

                try:
                    Console.print_info("Attempting to load existing particle membership data.")

                    pre_computed_membership = load_hdf5_files_with_xarray(
                        [make_complete_membership_file_path(settings.halo_membership_by_particle_directory, tag, settings.snipshots)],
                        particle_type.common_hdf5_name,
                        ["GroupNumber", "SubGroupNumber"],
                        dimension_sizes = { "snapshot_particle_index" : None }
                    )

                    group_numbers    = pre_computed_membership["GroupNumber"   ].isel(snapshot_particle_index = snapshot_particle_indexes).where(available_particles, NULL_INDEX).values
                    subgroup_numbers = pre_computed_membership["SubGroupNumber"].isel(snapshot_particle_index = snapshot_particle_indexes).where(available_particles, NULL_INDEX).values

                    if settings.include_so_region:
                        group_numbers = xr.where(group_numbers > 0, group_numbers, -group_numbers)
                    else:
                        exclusion_mask = group_numbers < 1
                        subgroup_numbers[exclusion_mask, 0] = NULL_INDEX
                        group_numbers   [exclusion_mask, 0] = NULL_INDEX


                    recorded_data["GroupNumber"   ][:, 0] = group_numbers
                    recorded_data["SubGroupNumber"][:, 0] = subgroup_numbers

                    fof_data_available = bool((recorded_data["GroupNumber"][:, 0] != NULL_INDEX).any())

                    loaded_from_membership_cache = True

                except Exception as e:
                    Console.print_error("    Failed.")
                    if Settings.debug:
                        raise e
                    else:
                        Console.print_info("    Falling back to catalogue membership files.")
                        loaded_from_membership_cache = False

            if not loaded_from_membership_cache:
                Console.print_info("Loading particle membership data from membership files.")

                structure_membership_information = load_catalogue_membership(
                    target_eagle_files,
                    do_gas         = particle_type == ParticleType.gas,
                    do_dark_matter = particle_type == ParticleType.dark_matter,
                    do_stars       = particle_type == ParticleType.star,
                    do_black_holes = particle_type == ParticleType.black_hole
                )[particle_type.common_hdf5_name]

                fof_data_available = False
                if structure_membership_information is not None:
                    # Find which of the available particles are being traced
                    identified_particle_indexes_in_membership = np.where(structure_membership_information["ParticleIDs"].isin(recorded_data["ParticleIDs"]))[0]
                    # Grab the IDs of those particles, as they are in the wrong order!
                    located_particle_ids = structure_membership_information["ParticleIDs"].isel(catalogue_membership_particle_index = identified_particle_indexes_in_membership)
                    if len(located_particle_ids) > 0:
                        fof_data_available = True

                if not fof_data_available:
                    recorded_data["GroupNumber"   ][:, 0] = NULL_INDEX
                    recorded_data["SubGroupNumber"][:, 0] = NULL_INDEX

                else:

                    # Particle info is in the wrong order, so make a mapping
                    # NOTE: this will be slow, but is implemented this way as the number of particles being traced is expected to be small
                    #TODO: find a better way of doing this lookup! (possibly, sort, searchsorted & ==)
                    located_particle_ids__numpy = np.array(located_particle_ids)
                    # Filled with uninitialised memory as values will be re-read only from the selected places - this is ok as a temp approach but would be better replaced with a memory efficient approach in case of many traced particles
                    membership_particle_indexes = np.empty(shape = len(recorded_data["ParticleIDs"]), dtype = np.uint64)
                    for membership_index, id in zip(identified_particle_indexes_in_membership, located_particle_ids__numpy):
                        membership_particle_indexes[np.where(recorded_data["ParticleIDs"] == id)[0][0]] = membership_index

                    # Get the index into the membership file for each traced particle, or the NULL_INDEX if it is not in any structure
                    membership_particle_indexes = xr.where(recorded_data["ParticleIDs"].isin(located_particle_ids), membership_particle_indexes, NULL_INDEX)

                    # Get the FOF entry NUMBER (index + 1) for each traced particle, provided it is in a FOF group
                    group_numbers = xr.where(membership_particle_indexes != NULL_INDEX, structure_membership_information["GroupNumber"].isel(catalogue_membership_particle_index = membership_particle_indexes), NULL_INDEX)

                    # Handle Spherical Overdensity particles (-ve group numbers for those in SO but not in FOF)
                    if settings.include_so_region:
                        group_numbers = xr.where(group_numbers > 0, group_numbers, -group_numbers)
                    else:
                        group_numbers = xr.where(group_numbers > 0, group_numbers, NULL_INDEX)

                    # Store the results (and force-compute if that hasn't already happened)
                    recorded_data["GroupNumber"][:, 0] = group_numbers.values

                    # Get the Subhalo index into the FOF group's subhaloes for each traced particle, provided it is in a subgroup
                    recorded_data["SubGroupNumber"][:, 0] = xr.where(group_numbers != NULL_INDEX, structure_membership_information["SubGroupNumber"].isel(catalogue_membership_particle_index = membership_particle_indexes), NULL_INDEX).values

            fof_data_available     = bool((recorded_data["GroupNumber"   ][:, 0] != NULL_INDEX).any()) # Needed as SO particles may have since been excluded!
            subhalo_data_available = bool((recorded_data["SubGroupNumber"][:, 0] != NULL_INDEX).any())

            #endregion Catalogue Indexes

            #---------------|
            # Load FOF data |
            #---------------|
            #region Load FOF

            if fof_data_available:

                fof_data = load_catalogue(
                    target_eagle_files,
                    group_fields = source_fields["fof"][particle_type] + ["FirstSubhaloID"],
                    subfind_fields = None
                )["FOF"]

                if fof_data is None:
                    fof_data_available = False

                else:

                    Console.print_info("Selecting FOF data from catalogue.")
                    # This is a delayed isel operation to allow for correct evaluation using xarray.where later on
                    selected_fof_data = fof_data.isel(catalogue_fof_index = da.array(recorded_data["GroupNumber"][:, 0].values - 1)).rename({ "catalogue_fof_index" : "ParticleIDs" })
                    valid_fof_particles = xr.where(recorded_data["GroupNumber"][:, 0] != NULL_INDEX, True, False)

            #endregion Load FOF

            #------------------------------|
            # Compute True Subhalo Indexes |
            #------------------------------|

            if subhalo_data_available:
                # Get the Subhalo entry index for each traced particle, provided it is in a subgroup
                recorded_data["SubGroupIndex"][:, 0] = xr.where(recorded_data["SubGroupNumber"][:, 0] != NULL_INDEX, selected_fof_data["FirstSubhaloID"] + recorded_data["SubGroupNumber"][:, 0], NULL_INDEX).values

            else:
                recorded_data["SubGroupIndex"][:, 0] = NULL_INDEX

            #---------------|
            # Grab FOF data |
            #---------------|
            #region Read FOF

            Console.print_info("Copying data.")

            for name, field, indexes, default_value in zip(field_names["fof"][particle_type], source_fields["fof"][particle_type], data_indexes["fof"][particle_type], default_values["fof"][particle_type]):

                Console.print_debug(f"{name} <- {field}{f"[{",".join(map(str, indexes))}]" if indexes is not None else ""}")

                if fof_data_available:
                    source = selected_fof_data[field]
                    if indexes is None:
                        recorded_data[name][:, 0] = source.where(valid_fof_particles, default_value).values
                    else:
                        target_indexes_only = source.isel({source.dims[1] : indexes})
                        if len(indexes) == 1:
                            target_indexes_only = target_indexes_only.squeeze(axis = 1)
                        recorded_data[name][:, 0] = target_indexes_only.where(valid_fof_particles, default_value).values

                else:
                    recorded_data[name][:, 0] = default_value

            #endregion Read FOF

            #-------------------|
            # Load Subhalo data |
            #-------------------|
            #region Load Subhalo

            if subhalo_data_available:

                subhalo_data = load_catalogue(target_eagle_files, group_fields = None, subfind_fields = source_fields["subhalo"][particle_type])["Subhalo"]
                # Handle any additional datasets in nested groups
                if nested_source_fields_are_present["subhalo"][particle_type]:
                    subsets = []
                    for group_path, fields in nested_source_fields_by_group["subhalo"][particle_type].items():
                        dataset = load_catalogue(target_eagle_files, group_fields = None, subfind_fields = fields, subfind_alternate_group_path = group_path)["Subhalo"]
                        if dataset is not None:
                            dataset = dataset.rename({
                                field_name : full_path_name
                                for field_name, full_path_name
                                in zip(fields, nested_source_original_field_paths["subhalo"][particle_type][group_path])
                            })
                            subsets.append(dataset)
                    merge_datasets = lambda *datasets: datasets[0].merge(merge_datasets(*datasets[1:])) if len(datasets) > 1 else datasets[0]
                    if len(source_fields["subhalo"][particle_type]) > 0 and subhalo_data is not None:
                        subhalo_data = merge_datasets(subhalo_data, *subsets)
                    else:
                        subhalo_data = merge_datasets(*subsets) if len(subsets) > 0 else None

                subhalo_data_available = subhalo_data is not None

            if subhalo_data_available:
                Console.print_info("Selecting Subhalo data from catalogue.")
                # This is a delayed isel operation to allow for correct evaluation using xarray.where later on
                selected_subhalo_data = subhalo_data.isel(catalogue_subhalo_index = da.array(recorded_data["SubGroupIndex"][:, 0].values)).rename({ "catalogue_subhalo_index" : "ParticleIDs" })
                valid_subhalo_particles = xr.where(recorded_data["SubGroupIndex"][:, 0] != NULL_INDEX, True, False)

            #endregion Load Subhalo

            #-------------------|
            # Grab Subhalo data |
            #-------------------|
            #region Read Subhalo

            Console.print_info("Copying data.")

            for name, field, indexes, default_value in zip(field_names["subhalo"][particle_type], source_fields["subhalo"][particle_type], data_indexes["subhalo"][particle_type], default_values["subhalo"][particle_type]):
                Console.print_debug(f"{name} <- {field}{f"[{",".join(map(str, indexes))}]" if indexes is not None else ""}")

                if subhalo_data_available:
                    source = selected_subhalo_data[field]
                    if indexes is None:
                        recorded_data[name][:, 0] = source.where(valid_subhalo_particles, default_value).values
                    else:
                        target_indexes_only = source.isel({source.dims[1] : indexes})
                        if len(indexes) == 1:
                            target_indexes_only = target_indexes_only.squeeze(axis = 1)
                        recorded_data[name][:, 0] = target_indexes_only.where(valid_subhalo_particles, default_value).values

                else:
                    recorded_data[name][:, 0] = default_value

            #endregion Read Subhalo

            #------------|
            # Write data |
            #------------|

            write_data(tag, particle_type, recorded_data)

            #---------------|
            # Cache indexes |
            #---------------|

            if try_use_reorder_cache:
                last_tag = tag
                last_snapshot_indexes[:] = snapshot_particle_indexes

    Console.print_info("DONE")
    return
