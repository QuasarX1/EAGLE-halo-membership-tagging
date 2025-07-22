import glob

import xarray as xr

def load_hdf5_files_with_xarray(
    files: tuple[str, ...]|list[str],
    hdf5_group_path: str|None = None,
    datasets: list[str]|None = None,
    coordinate_dataset: str|None = None
) -> xr.Dataset:

    if datasets is None:
        datasets = ["ParticleIDs"]
    else:
        datasets = list(datasets)

    if coordinate_dataset not in datasets:
        datasets.insert(0, coordinate_dataset)

    # Define a function that handles pre-processing of individual files before concatenation
    def preprocess(file_data: xr.Dataset) -> xr.Dataset:

        ## If the dataset is completely empty (no variables), skip it
        #if len(file_data.data_vars) == 0:
        #    return xr.Dataset({key: (("file_order",), []) for key in file_data.data_vars})  # xarray will skip this file when combining

        # Rename the longest (main) dimension to 'file_order' so xarray can concat on it
        if len(file_data.dims) > 0 and "phony_dim_0" in file_data.dims:
            #print(file_data.dims)
            #main_dim = max(file_data.dims, key = file_data.dims.get)
            #file_data = file_data.rename_dims({ main_dim : "file_order" })
            file_data = file_data.rename_dims({ "phony_dim_0" : "file_order" })
            #print(file_data.dims)
            #print()
        else:
            file_data.expand_dims("file_order", axis = 0)

        # Drop any datasets not requested
        drop_vars = [var for var in file_data.data_vars if var not in datasets]
        file_data = file_data.drop_vars(drop_vars)

        return file_data
    
    # Open all files as one virtual dataset using xarray + dask
    combined_dataset = xr.open_mfdataset(
        files,
        group      = hdf5_group_path,
        preprocess = preprocess,
        combine    = "nested",        # Just stack datasets without trying to align them
        concat_dim = "file_order",    # This was set in `preprocess`
        chunks     = "auto",          # Respect internal HDF5 chunking
        phony_dims = "access",        # Fabricate dimensions if not present
        data_vars  = "minimal",       # Only combine variables that were kept
        compat     = "override",      #
        coords     = "minimal",       # Don’t try to infer or align coords across files
        engine     = "h5netcdf"
    )

    if coordinate_dataset is not None and coordinate_dataset in combined_dataset.data_vars:
        #combined_dataset = combined_dataset.assign_coords({ "id" : combined_dataset[coordinate_dataset] }).set_xindex("id")
        combined_dataset = combined_dataset.set_coords(coordinate_dataset).set_xindex(coordinate_dataset)

    return combined_dataset

def load_hdf5_pattern_with_xarray(
    filepath_template: str,
    hdf5_group_path: str|None = None,
    datasets: list[str]|None = None,
    coordinate_dataset: str|None = None
) -> xr.Dataset:

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

        
        return load_hdf5_files_with_xarray(chunks, hdf5_group_path, datasets, coordinate_dataset)

    else:
        
        return load_hdf5_files_with_xarray([filepath_template], hdf5_group_path, datasets, coordinate_dataset)