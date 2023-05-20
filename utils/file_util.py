import logging
import pandas as pd
import os
import sys
import utils.misc_util as miscu
sys.path.append(os.getcwd())
from utils.data_storage import DataStorage
from utils.log_util import log_trace


class FileDataStorage(DataStorage):
    """ File to read from and write to local files. Write
        function can determine the next best file path to
        write to using annotated static methods. Extends
        DataStorage superclass.
    """

    def __init__(self):
        """ Initializes this subclass and calls parent
            class initialization
        """
        super(DataStorage, self).__init__()

    @log_trace
    def read(self, config):
        """
        Read file, along with validating provided path.
        param config: dict
            configuration for the specific file to read in
        returns: pandas dataframe
        """
        description = miscu.eval_elem_mapping(config, 'description')
        path = miscu.eval_elem_mapping(config, 'path')
        file_type = miscu.eval_elem_mapping(config, 'file_type', default_value='csv')
        separator = miscu.eval_elem_mapping(config, 'separator', default_value=',')
        skip_rows = miscu.eval_elem_mapping(config, 'skip_rows', default_value=0)
        use_cols = miscu.eval_elem_mapping(config, 'use_cols', default_value=None)
        sheet_name = miscu.eval_elem_mapping(config, 'sheet_name', default_value=0)

        df_target = None
        if FileDataStorage.validate_path(path):
            if file_type.lower() == 'csv':
                # Read csv based file.
                df_target = pd.read_csv(path,
                                        sep=separator,
                                        skiprows=skip_rows,
                                        usecols=use_cols,
                                        encoding='unicode_escape')
            elif file_type.lower() == 'excel':
                # Read Excel based file.
                if len((pd.ExcelFile(path)).sheet_names) > 1:
                    df_target = pd.read_excel(path,
                                              skiprows=skip_rows,
                                              usecols=use_cols,
                                              sheet_name=sheet_name,
                                              engine="openpyxl")
                else:
                    df_target = pd.read_excel(path,
                                              skiprows=skip_rows,
                                              usecols=use_cols,
                                              engine="openpyxl")

        logging.info(f'{description} records <{len(df_target.index)}> were read from <{path}>')
        return df_target

    @log_trace
    def write(self, config, df):
        """ Write dataframe to destination path and filename passed by caller

        param config: dict
            Configuration for write destination
        param df: pandas dataframe
            the dataframe to be written (pandas form)
        param mode: str
            'new' is default while anything else will overwrite any
            existing files with the passed output file name
        return: None
            This method saves to a designated file path
        """
        description = miscu.eval_elem_mapping(config, 'description')
        path = miscu.eval_elem_mapping(config, 'path')
        file_type = miscu.eval_elem_mapping(config, 'file_type', default_value='excel')
        separator = miscu.eval_elem_mapping(config, 'separator', default_value=',')
        mode = miscu.eval_elem_mapping(config, 'mode', default_value='new')

        # Get a final path based on caller provided parameters
        final_path = FileDataStorage.get_avail_path(path, file_type, mode)

        # This will write all dataframes to a labeled sheet in one excel file
        # Write to either a csv or xlsx file if single df or xlsx for multiple df
        # Note: this logic can handle a list of dataframes in case of multiple
        # sheets/transformations
        if isinstance(df, list):
            if file_type == 'csv':
                raise IOError("CSV file can only handle one transformation. Please elect 'excel' "
                              "for multiple transformations.")
            num_records = 0
            transform_types = miscu.eval_elem_mapping(config, 'sheet_naming')
            with pd.ExcelWriter(final_path) as writer:
                for dataframe, transform_type in zip(df, transform_types):
                    dataframe.to_excel(writer, index=False, sheet_name=transform_type)
                    num_records += len(dataframe.index)
        else:
            if file_type == 'csv':
                df.to_csv(final_path, sep=separator, index=False)
            else:
                df.to_excel(final_path, index=False)
            num_records = len(df.index)

        logging.info(f'{description} records <{num_records}> were written to <{final_path}>')
        return

    @staticmethod
    def validate_path(path, attribute_check="filepath"):
        """ Validate provided directory and path.

        param path: Fully qualified file path
        param attribute_check: directory or file to validate
            optional choices ('filepath', 'directory')
        return: bool
            resulted validation; either true or raise an exception
        """
        if attribute_check == 'directory':
            dir_path = os.path.dirname(path)
            if not os.path.isdir(dir_path):
                logging.error(f'Provided directory path is invalid: <{dir_path}>')
                raise NotADirectoryError(f'Provided directory path is invalid: <{dir_path}>')
        if attribute_check == 'filepath':
            if not os.path.isfile(path):
                logging.error(f'Provided file path is invalid: <{path}>')
                raise FileNotFoundError(f'Provided file path is invalid: <{path}>')
        return True

    @staticmethod
    def get_avail_path(path, file_type, mode):
        """ Find an available path for saving a file

        param path: str
            intended save destination
        param file_type: str
            intended write format
            options - 'excel' or 'csv'
        return: str
            Available path for dataframe save
        """
        # Get list of existing files
        files = os.listdir(os.path.dirname(path))

        # If you want a new file but the path given pre-exists,
        # get a new path. All other situations can use the path
        # as provided.
        if mode == 'new' and path.split('/')[-1] in files:
            # Get the root title intended by the caller
            final_file_title = FileDataStorage.get_title_without_suffix(path)

            # Convert file_type to correct extension
            file_type = 'xlsx' if file_type == 'excel' else file_type

            # Get an available file number
            next_number = FileDataStorage.get_avail_version_number(file_type,
                                                                   final_file_title,
                                                                   files)

            # Return a destination path for the written file
            return os.path.join(os.path.dirname(os.path.abspath(path)),
                                final_file_title + next_number + '.' + file_type)
        return path

    @staticmethod
    def get_title_without_suffix(provided_path_with_suffix):
        """ Retrieve root title of intended file name

        param provided_path_with_suffix: str
            the intended save path
            can handle abs path or just file name
        return: str
            returns root title of a file
        """
        # This drops the file extension
        title_to_return = provided_path_with_suffix.split('/')[-1].split('.')[0]

        # This drops any numeric suffixes
        if '_' in title_to_return:
            title_to_return = title_to_return.split('_')[0]

        # returns 'X' from 'X_#.csv' file name convention
        return title_to_return

    @staticmethod
    def get_avail_version_number(file_type, final_file_title, files_to_traverse):
        """ Retrieve the next available numeric suffix for final titling

        param path: str
            intended save destination
        param file_type: str
            intended write format
            options - 'excel' or 'csv'
        param final_file_title: str
            root title of intended file naming
        returns: str
            returns the next available numeric suffix within
            path directory
        """
        file_numbers = []
        file_count = 0

        # Checking existing files for the next available suffix
        # version number
        for file in files_to_traverse:
            curr_file_title = FileDataStorage.get_title_without_suffix(file)
            # File is of the appropriate type and root title convention
            if file.endswith('.' + file_type) \
                    and curr_file_title == final_file_title:
                full_file_name = file.split('\\')[-1]  # 'X_#.csv' convention
                file_name = full_file_name.split('.')[0]  # 'X_#' convention
                file_count += 1
                try:
                    number = int(file_name.split('_')[1])
                    file_numbers.append(number)
                except IndexError:
                    continue

        # If we need an unused numeric suffix
        if len(file_numbers) != 0:
            return '_' + str(FileDataStorage.first_missing_num(file_numbers))
        # If we only found a previous file version without a numeric suffix
        # e.g., X.csv exists but no other versions exist
        elif len(file_numbers) == 0 and file_count != 0:
            return '_' + str(1)
        # If we didn't find any pre-existing files with the
        # intended root title, including those without a numeric suffix
        # e.g., no 'X.csv' or 'X_#.csv'
        else:
            return ''

    @staticmethod
    def first_missing_num(list_of_nums):
        """ Finds and returns the next available numeric
            suffix used for file name

        param list_of_nums: list of ints
            list of numeric suffixes already existing
            in the intended file path
        returns: int
            returns next available numeric suffix
        """
        numbers = set(list_of_nums)
        for num in range(min(numbers), max(numbers)):
            if num not in numbers:
                return num
        return max(numbers) + 1
