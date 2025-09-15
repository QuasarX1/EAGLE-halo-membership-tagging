import glob

from QuasarCode import Console
import xarray as xr

def load_hdf5_files_with_xarray(
    files: tuple[str, ...]|list[str],
    hdf5_group_path: str|None = None,
    datasets: list[str]|None = None,
    coordinate_dataset: str|None = None,
    override_chunks_in_all_dimensions: int|str|None = "auto",
    dimension_sizes: dict[str, int|None]|None = None,
    concatenation_dimension_index: int|None = None # Set this if the length of the concatenation dimension matches the length of another dimension
) -> xr.Dataset:
    
    if dimension_sizes is not None and len([v for v in dimension_sizes.values() if v is None]) > 1:
        raise TypeError("More than one dimension specified as the concatenation dimension.")

    if datasets is None:
        datasets = ["ParticleIDs"]
    else:
        datasets = list(datasets)

    if coordinate_dataset not in datasets:
        datasets.insert(0, coordinate_dataset)

    concat_dimension_name = "file_order"
    if dimension_sizes is not None and None in dimension_sizes.values():
        concat_dimension_name = {value: key for key, value in dimension_sizes.items()}[None]

    # Define a function that handles pre-processing of individual files before concatenation
    def preprocess(file_data: xr.Dataset) -> xr.Dataset:

        target_dim_name: str # The default name of the dimension used to concatenate on

        # Drop any datasets not requested
        drop_vars = [var for var in file_data.data_vars if var not in datasets]
        file_data = file_data.drop_vars(drop_vars)

        # Handle dimension names

        if len(file_data.sizes) > 0:
            duplicate_names: dict[int, list[str]] = {}
            for i, (dimension_name, dimension_length) in enumerate(file_data.sizes.items()):
                if concatenation_dimension_index is not None and i == concatenation_dimension_index:
                    continue # This dimension cannot be merged - it is marked explicitly for concatenation
                if dimension_length not in duplicate_names:
                    duplicates = [str(name) for j, (name, length) in enumerate(file_data.sizes.items()) if length == dimension_length and (concatenation_dimension_index is None or j != concatenation_dimension_index)]
                    if len(duplicates) > 1:
                        duplicate_names[dimension_length] = duplicates
            if len(duplicate_names) > 0:
                dimension_merges = {}
                for name_list in duplicate_names.values():
                    for old_name in name_list[:1]:
                        dimension_merges[old_name] = name_list[0]
                file_data = file_data.swap_dims(dimension_merges)

        if dimension_sizes is not None:
            possible_named_dimensions = { length : name for name, length in dimension_sizes.items() }
            dimension_names = file_data.sizes.keys()
            name_updates: dict[str, str] = {}
            for i, old_dimension_name in enumerate(dimension_names):
                if concatenation_dimension_index is not None and i == concatenation_dimension_index:
                    target_dim_name = str(old_dimension_name)
                    continue
                dimension_length = file_data.sizes[old_dimension_name]
                if dimension_length in possible_named_dimensions:
                    name_updates[str(old_dimension_name)] = possible_named_dimensions[dimension_length]
            if len(name_updates) > 0:
                #Console.print_debug(f"Applying dimension renaming: {name_updates}")
                file_data = file_data.rename_dims(name_updates)

        ## If the dataset is completely empty (no variables), skip it
        #if len(file_data.data_vars) == 0:
        #    return xr.Dataset({key: (("file_order",), []) for key in file_data.data_vars})  # xarray will skip this file when combining

        # Rename the longest (main) dimension to 'file_order' so xarray can concat on it
        if len(file_data.sizes) > 0:
            available_dim_names = list(file_data.sizes.keys())
            if dimension_sizes is not None:
                invalid_names = [name for name, size in dimension_sizes.items() if size is not None]
                available_dim_names = [name for name in available_dim_names if name not in invalid_names]
                if len(available_dim_names) == 0:
                    Console.print_debug(f"Data dimensions after renaming: {file_data.sizes.keys()}")
                    Console.print_debug(f"Provided dimension sizes: {dimension_sizes}")
                    Console.print_debug(f"Identified invalid dimension names: {invalid_names}")
                    raise ValueError("No unnamed dimension left to concatenate on!")
            dim_values = []
            for dim_name in available_dim_names:
                try:
                    dim_values.append(int(str(dim_name).rsplit("_", maxsplit = 1)[-1]))
                except ValueError:
                    pass
            if concatenation_dimension_index is None: # otherwise this is set during the dimension renaming
                target_dim_name = str(available_dim_names[min(range(len(available_dim_names)), key = lambda x: dim_values[x]) if len(dim_values) > 0 else available_dim_names[0]])
            #main_dim = max(file_data.sizes, key = file_data.sizes.get)
            #file_data = file_data.rename_dims({ main_dim : "file_order" })
            file_data = file_data.rename_dims({ target_dim_name : concat_dimension_name })
            #print(file_data.sizes)
            #print()
        else:
            file_data = file_data.expand_dims(concat_dimension_name, axis = 0)

        return file_data

    # Open all files as one virtual dataset using xarray + dask
    combined_dataset = xr.open_mfdataset(
        files,
        group      = hdf5_group_path,
        preprocess = preprocess,
        combine    = "nested",                          # Just stack datasets without trying to align them
        concat_dim = concat_dimension_name,             # This was set in `preprocess`
        chunks     = override_chunks_in_all_dimensions, # Respect internal HDF5 chunking
        phony_dims = "access",                          # Fabricate dimensions if not present
        data_vars  = "minimal",                         # Only combine variables that were kept
        compat     = "override",
        coords     = "minimal",                         # Don’t try to infer or align coords across files
        engine     = "h5netcdf",
        #parallel = True
    )

    if coordinate_dataset is not None and coordinate_dataset in combined_dataset.data_vars:
        #combined_dataset = combined_dataset.assign_coords({ "id" : combined_dataset[coordinate_dataset] }).set_xindex("id")
        combined_dataset = combined_dataset.set_coords(coordinate_dataset).set_xindex(coordinate_dataset)

    return combined_dataset

def load_hdf5_pattern_with_xarray(
    filepath_template: str,
    hdf5_group_path: str|None = None,
    datasets: list[str]|None = None,
    coordinate_dataset: str|None = None,
    skip_values: list[str]|None = None,
    override_chunks_in_all_dimensions: int|str|None = "auto",
    dimension_sizes: dict[str, int|None]|None = None,
    concatenation_dimension_index: int|None = None # Set this if the length of the concatenation dimension matches the length of another dimension
) -> xr.Dataset:
    
    if skip_values is not None and len(skip_values) == 0:
        skip_values = None

    if "{" in filepath_template or "*" in filepath_template:

        if "*" not in filepath_template:
            generic_filepath = filepath_template.format("*")
            wildcard_position = len(generic_filepath.split("*")[0])
            length_after_wildcard = len(generic_filepath.split("*")[1])
            chunks = glob.glob(generic_filepath)
            chunks.sort(key = lambda x: int(x[wildcard_position : -length_after_wildcard] if length_after_wildcard != 0 else x[wildcard_position:])) # sort the chunks by the number in the filename

        else:
            wildcard_position = len(filepath_template.split("*")[0])
            length_after_wildcard = len(filepath_template.split("*")[1])
            chunks = glob.glob(filepath_template)
            chunks.sort(key = lambda x: int(x[wildcard_position : -length_after_wildcard] if length_after_wildcard != 0 else x[wildcard_position:]))

        if skip_values is not None:
            chunks = [value for value in chunks if value[wildcard_position : -length_after_wildcard] not in skip_values]
        
        return load_hdf5_files_with_xarray(chunks, hdf5_group_path, datasets, coordinate_dataset, override_chunks_in_all_dimensions, dimension_sizes, concatenation_dimension_index)

    else:

        return load_hdf5_files_with_xarray([filepath_template], hdf5_group_path, datasets, coordinate_dataset, override_chunks_in_all_dimensions, dimension_sizes, concatenation_dimension_index)