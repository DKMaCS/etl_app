import os
import sys
import pandas as pd
import datetime
sys.path.append(os.getcwd())
import utils.misc_util as miscu
from utils.file_util import FileDataStorage
from utils.log_util import log_trace


def apply_dtype_feature(df, config):
    """
    ETL feature to apply data types to dataframe columns and limit columns to ones specified
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    Sample:
    "apply_dtype": {
        "INSURANCE_CODE": "str",
        "INSURANCE_AMOUNT": "float",
        "CLIENT_TYPE": "int"
    }
    """
    if config and isinstance(config, dict):
        for column_key, type_value in config.items():
            if column_key in df:
                # str type.
                if type_value is str or type_value == 'str':
                    df[column_key] = df[column_key].fillna('')
                    df[column_key] = df[column_key].astype(str)
                # int type.
                elif type_value is int or type_value == 'int':
                    df[column_key] = df[column_key].fillna(0)
                    df[column_key] = df[column_key].astype(int)
                # float type.
                elif type_value is float or type_value == 'float':
                    df[column_key] = df[column_key].fillna(0.0)
                    df[column_key] = df[column_key].astype(float)
                # datetime type
                elif type_value is datetime.date or type_value == 'datetime.date':
                    df[column_key] = pd.to_datetime(df[column_key])
            else:
                raise KeyError(f'Column <{column_key}> is missing from given dataframe')

        # Limit dataframe to specified columns.
        df = df[list(config.keys())]
    return df


@log_trace
def mapping_feature(df, config):
    """
    ETL feature to merge given dataframe with extracted mapping dataframe
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    """
    df_mapping = read_feature(config['read'])
    df_target = pd.merge(df, df_mapping, how='left',
                         left_on=miscu.eval_elem_mapping(config, 'left_on'),
                         right_on=miscu.eval_elem_mapping(config, 'right_on'))
    df_target['Region'] = df_target['Region'].fillna('Other')
    df_target.drop(columns=miscu.eval_elem_mapping(config, 'right_on'), inplace=True)

    return df_target


@log_trace
def read_feature(config):
    """
    ETL feature to read a file, based on provided ETL configuration section
    This is a composite feature, since it can call apply_dtype_feature, if appropriate config section exists
    :param config: dict; Provided configuration mapping
    :return: pd.DataFrame; Resulted dataframe
    """
    df_target = FileDataStorage().read(config=config)

    df_target.columns = df_target.columns.str.strip()

    # Call apply_dtype_feature, if appropriate config section exists
    apply_dtype_config = miscu.eval_elem_mapping(config, 'apply_dtype')
    if apply_dtype_config:
        df_target = apply_dtype_feature(df_target, apply_dtype_config)

    return df_target


@log_trace
def df_col_mods_feature(df, config):
    """ ETL feature to rename, reorder, and add static columns
        to given dataframe, df

    param df: pandas dataframe
        dataframe to make changes to
    param config: dict
        configuration of column changes
    returns: df
        dataframe with changes made
    """
    # Rename columns
    output_config = miscu.eval_elem_mapping(config, 'output')
    col_change_map = miscu.eval_elem_mapping(output_config, 'col_rename')
    static_col_data = miscu.eval_elem_mapping(output_config, 'assign_static')
    df.rename(columns=col_change_map, inplace=True)

    # Assign static columns
    for col, value in static_col_data.items():
        df[col] = value
    return df


@log_trace
def write_feature(config, df):
    """ Write given df to local destination using the correct
        configuration and given worksheet title

    param config: dict
        file write configurations
    param df: pandas dataframe
        dataframe to write to disk
    param sheet_destination: str
        title of worksheet to use or create
    returns: df
        dataframe with changes made
    """

    FileDataStorage().write(config=config, df=df)
    return


@log_trace
def transform_feature(df, config):
    """ Make transformations to df

    param df: pandas dataframe
        df to be transformed
    param config:
        map of transformation configs
    returns: pandas dataframe
        df with necessary transformations
    """

    # Get transformation configuration details
    output_transform_configs = miscu.eval_elem_mapping(config, 'output')
    col_transformation_configs = miscu.eval_elem_mapping(output_transform_configs, 'col_transforms')
    column_to_add = miscu.eval_elem_mapping(col_transformation_configs, "add")
    columns_to_use_for_transformation = miscu.eval_elem_mapping(col_transformation_configs, "from")

    # Get destination sheet names
    dest_sheet_names = miscu.eval_elem_mapping(output_transform_configs, 'sheet_naming')

    # Get destination columns names
    dest_col_names = miscu.eval_elem_mapping(output_transform_configs, 'dest_cols')

    # Begin transformations
    # First adding a line item for total price (quantity * unit price)
    # Performing the necessary aggregations
    # Returning final dataframes
    df[column_to_add] = df[columns_to_use_for_transformation[0]] * df[columns_to_use_for_transformation[1]]
    list_of_transformed_df = []
    for category in dest_sheet_names:
        transforming_df = df.copy()
        transforming_df = aggregate_feature(transforming_df, category, config)
        transforming_df[dest_col_names[1]] = 100 * transforming_df[dest_col_names[0]] / transforming_df[
                                                dest_col_names[0]].sum()
        list_of_transformed_df.append(transforming_df)

    return list_of_transformed_df


@log_trace
def aggregate_feature(df, category, config):
    """ Aggregation helper using groupby or pivot tables

    param df: pandas dataframe
        df to be transformed
    param category: str
        aggregation index name
    param config: dict
        transformation configurations
    returns: pandas dataframe
        df with necessary transformations
    """

    output_configs = miscu.eval_elem_mapping(config, 'output')
    col_transforms = miscu.eval_elem_mapping(output_configs, 'col_transforms')
    add_col = miscu.eval_elem_mapping(col_transforms, 'add')
    dest_cols = miscu.eval_elem_mapping(output_configs, 'dest_cols')

    agg_configs = miscu.eval_elem_mapping(config, 'aggregate')
    agg_method = miscu.eval_elem_mapping(agg_configs, 'aggfunc')
    agg_type = miscu.eval_elem_mapping(agg_configs, 'type')

    if agg_type.lower() == 'groupby':
        groupby_table = df.copy()
        groupby_table[dest_cols[0]] = groupby_table.groupby([category])[add_col].transform(agg_method)
        groupby_table = groupby_table.drop_duplicates(subset=[category])[[category, dest_cols[0]]]
        return groupby_table
    elif agg_type.lower() == 'pivot':
        pivot_table = df.pivot_table(index=category, aggfunc={add_col: agg_method})
        pivot_table = pivot_table.rename(columns={add_col: dest_cols[0]})
        pivot_table.insert(0, category, pivot_table.index)
        return pivot_table
