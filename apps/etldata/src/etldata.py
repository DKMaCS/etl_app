import os
import sys
sys.path.append(os.getcwd())
import utils.etl_util as etlu
import utils.misc_util as miscu
import argparse
import json
import logging
from types import SimpleNamespace as Namespace
from utils.log_util import log_trace

RETURN_SUCCESS = 0
RETURN_FAILURE = 1
APP = 'EtlData utility'


def main(argv):
    try:
        # Parse command line arguments.
        args, process_name, feature_type, feature_config = _interpret_args(argv)

        # Initialize standard logging \ destination file handlers.
        std_filename = vars(args)['log_path']
        logging.basicConfig(filename=std_filename, filemode='a',
                            format='%(asctime)s - %(message)s',
                            level=logging.INFO)
        logging.info('')
        logging.info(f'Entering {APP}')

        # Preparation step.
        mapping_args = miscu.convert_namespace_to_dict(args)
        mapping_conf = miscu.convert_namespace_to_dict(feature_config)

        # Workflow steps.
        if feature_type == 'extraction':
            run_extraction(mapping_args, mapping_conf)
        elif feature_type == 'transformation':
            run_transformation(mapping_args, mapping_conf)
        else:
            logging.warning(f'Incorrect feature type: [{feature_type}]')

        logging.info(f'Leaving {APP}')

        return RETURN_SUCCESS
    except FileNotFoundError as nf_error:
        logging.info(f'Leaving {APP} incomplete with errors')
        return f'ERROR: {str(nf_error)}'
    except KeyError as key_error:
        logging.info(f'Leaving {APP} incomplete with errors')
        return f'ERROR: {key_error.args[0]}'
    except Exception as gen_exc:
        logging.info(f'Leaving {APP} incomplete with errors')
        raise gen_exc


def _interpret_args(argv):
    """
    Read, parse, and interpret given command line arguments.
    Also, define default value.
    :param argv: Given argument parameters.
    :return: Full mapping of arguments, including all default values.
    """
    arg_parser = argparse.ArgumentParser(APP)
    arg_parser.add_argument('-log', dest='log_path', help='Fully qualified logging file')
    arg_parser.add_argument('-process', dest='process', help='Process type', required=True)
    arg_parser.add_argument('-mode', dest='mode', help='Overwrite or create new when writing choice')

    # Extract and interpret rest of the arguments, using static config file, based on given specific feature.
    process_arg = argv[argv.index('-process') + 1]
    process_args = process_arg.rsplit('_', 1)
    process_name = process_args[0]
    feature_type = process_args[1]
    current_path = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(current_path, f'..\\config\\{process_name}.json')) as file_config:
        mapping_config = json.load(file_config, object_hook=lambda d: Namespace(**d))
        if feature_type == 'extraction':
            feature_config = vars(mapping_config.extraction)
        elif feature_type == 'transformation':
            feature_config = vars(mapping_config.transformation)

        feature_args = vars(mapping_config.feature_args)
        # Add necessary arguments to <arg_parser> instance, using static JSON-based configuration.
        if feature_args:
            for key, value in feature_args.items():
                if isinstance(value, Namespace):
                    value = vars(value)
                arg_parser.add_argument(key, dest=value['dest'],
                                        help=value['help'],
                                        required=value['required'])
    return arg_parser.parse_args(argv), process_name, feature_type, feature_config


@log_trace
def run_extraction(args, config):
    """ Create dataframe object populated with the data from source file

    params args
        List of user passed arguments from terminal
    params config
        Extraction settings configuration from json
    returns: Pandas dataframe
        Extracted dataframe
    """

    # --------------------------------
    # Input section
    # --------------------------------

    # Prepare additional input parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <input> config section.
    input_update_with = {'path': miscu.eval_elem_mapping(args,
                                                         'input_path'),
                         'description': config['description']}
    input_config = miscu.eval_elem_mapping(config, 'input')
    input_read_config = miscu.eval_update_mapping(input_config,
                                                  "read",
                                                  input_update_with)

    # Run read ETL feature.
    df_target = etlu.read_feature(input_read_config)

    # Engage plugin from <input> config section, if available.
    input_plugin = miscu.eval_elem_mapping(input_config, "plugin")
    if input_plugin:
        df_target = input_plugin(df_target)

    # --------------------------------
    # Mapping section
    # --------------------------------

    # Prepare additional mapping parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <mapping> config section.
    mapping_update_with = {'path': miscu.eval_elem_mapping(args,
                                                           'mapping_path'),
                           'description': config['description']}
    mapping_config = miscu.eval_elem_mapping(config, 'mapping')
    mapping_read_config = miscu.eval_update_mapping(mapping_config,
                                                    'read',
                                                    mapping_update_with)

    # Run mapping ETL feature.
    df_target = etlu.mapping_feature(df_target, mapping_config)

    # --------------------------------
    # Column Modifications
    # --------------------------------

    etlu.df_col_mods_feature(df_target, config)

    # --------------------------------
    # Output section
    # --------------------------------

    # Extracting intended destination and description of write file
    output_update_with = {'path': miscu.eval_elem_mapping(args,
                                                          'output_path'),
                          'description': config['description'],
                          'mode': miscu.eval_elem_mapping(args,
                                                          'mode')}

    # Update json configuration file with the
    # intended destination and description passed
    output_write_config = miscu.eval_update_mapping(config,
                                                    'output',
                                                    output_update_with)

    # Writing final dataframe to /data folder
    etlu.write_feature(output_write_config, df_target)

    return df_target


@log_trace
def run_transformation(args, config):
    """ Transform data based on passed arguments and configurations

    params args
        List of user passed arguments from terminal
    params config
        Extraction settings configuration from json
    returns: Pandas dataframe
        Extracted dataframe
    """

    # --------------------------------
    # Input section
    # --------------------------------

    # Prepare additional input parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <input> config section.
    input_update_with = {'path': miscu.eval_elem_mapping(args,
                                                         'input_path'),
                         'description': config['description']}
    input_config = miscu.eval_elem_mapping(config, 'input')
    input_read_config = miscu.eval_update_mapping(input_config,
                                                  "read",
                                                  input_update_with)

    # Run read ETL feature.
    df_target = etlu.read_feature(input_read_config)

    # Engage plugin from <input> config section, if available.
    input_plugin = miscu.eval_elem_mapping(input_config, "plugin")
    if input_plugin:
        df_target = input_plugin(df_target)

    # --------------------------------
    # Transformation section
    # --------------------------------

    # Transforming data according to transformations configurations json file
    list_of_transformed_df = etlu.transform_feature(df_target, config)

    # --------------------------------
    # Output section
    # --------------------------------

    # Extracting intended destination and description of write file
    output_update_with = {'path': miscu.eval_elem_mapping(args,
                                                          'output_path'),
                          'description': config['description'],
                          'mode': miscu.eval_elem_mapping(args,
                                                          'mode')}

    # Update json configuration file with the
    # intended destination and description passed
    output_write_config = miscu.eval_update_mapping(config,
                                                    'output',
                                                    output_update_with)

    # Writing final dataframe to /data folder
    # This will write all dataframes to a labeled sheet in one excel file
    etlu.write_feature(output_write_config, list_of_transformed_df)

    return df_target


if __name__ == '__main__':
    # Call main process.
    sys.exit(main(sys.argv[1:]))
