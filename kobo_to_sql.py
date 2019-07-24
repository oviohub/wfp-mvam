import os
import re

import requests
import pandas as pd

import utils

# Survey name
SURVEY_NAME = 'PNG - mVAM REVVG'

# Make sure the admin area file is available in ./data.
DATA_DIR = './data'
CLEAN_DATA_FILE = 'png_round6_clean_data.csv'

SQL_SCHEMA_FILE = './resources/sql_tables_structure.xlsx'
SQL_SCHEMA_SHEET_NAME = 'Sheet1'

LABELS_FILE = './resources/kobo_form_structure.xls'
LABELS_SHEET = 'choices'


# Columns with multiple choices
# Column, new column
CM = {
    'CMWaterConstr': {'column_name': 'CMWaterConstrWaterConstr', 'label_key': 'WaterConstr'},
    'CMFood': {'column_name': 'HHIllType_chsickness', 'label_key': 'sickness'},
    'HHIllType_adF': {'column_name': 'HHIllType_adFsickness', 'label_key': 'sickness'},
    'HHIllType_adM': {'column_name': 'HHIllType_adMsickness', 'label_key': 'sickness'},
    'HHDisabledWho': {'column_name': 'HHDisabledWhoDisableWho', 'label_key': 'DisableWho'},
    'RESPComsMeanBest': {'column_name': 'RESPComsMeanBestComsMeans', 'label_key': 'ComsMeans'},
    'CMInfoNeeds': {'column_name': 'CMInfoNeedsInfoNeeds', 'label_key': 'InfoNeeds'},
}

SC_LABELS_ENDS_WITH = {
    'SupplyRank': 'RankAvail',
    'CMFood': 'SRf',
    'WaterCollectWho': 'WaterWho',
    'WaterConstr': 'WaterConstr',
    'PercHunger': 'RankPct',
    'FoodRank': 'RankPct',
    'CMFarmGardRank': 'RankPct',
    'CMFarmGardProdChg': 'Chg',
    # 'Rank': 'RankPct',
}

SC_LABELS = {
    'CMFoodSupplyRank': 'RankAvail',
    'CMFood': 'SRf',
    'CMWaterSupplyRank': 'RankAvail',

    # 'WaterConstr': 'RankAvail',
    # 'CMFoodSupplyRank': 'RankAvail',
}


def get_sc_list_name(column_name):
    for key, value in SC_LABELS_ENDS_WITH.iteritems():
        if column_name.endswith(key):
            return value

    raise KeyError(('No Value Found for ' + column_name))


def expand_multiple_choices(column, data, CM, choice_labels):
    print('Expanding multiple choices for ' + column)


def is_label(column_name, columns, data_columns):
    """
        If the column name ends with two, and the column follows
        an existing data column, then it is safe to assume that it
        is a label column.
    """
    index = columns.index(column_name)
    return (
        column_name[-1] == '2'
        and columns[index-1] in data_columns
    )


def load_choice_labels():
    choice_labels_dataframe = pd.read_excel(
        LABELS_FILE, sheet_name=LABELS_SHEET)
    choice_labels_dataframe.drop(['ADMIN1Code', 'ADMIN2Code'], axis=1)

    choice_labels = {}
    for i in choice_labels_dataframe['list_name'].unique():
        choice_labels[i] = [{choice_labels_dataframe['name'][j]: choice_labels_dataframe['label'][j]}
                            for j in choice_labels_dataframe[choice_labels_dataframe['list_name'] == i].index]

    return choice_labels


def is_admin_label(column_name):
    regex = re.compile("ADMIN(\d+)Name")
    return regex.findall(column_name)


def get_label(list_name, name, choice_labels):
    """Reshape list of label dicts as one dict."""
    # print(list_name, name)
    label_dicts = dict(
        (key, d[key])
        for d in choice_labels[list_name]
        for key in d
    )
    # print(label_dicts)

    try:
        return label_dicts[name]
    except KeyError:
        try:
            return label_dicts[str(int(name))]
        except ValueError:
            # TODO - Ask what to do in this situation.
            # Should we unpack the data and concat with ";" ?
            return 'Multiple Values'


def main():
    pd.options.mode.chained_assignment = None

    print('Loading clean data...')
    data = pd.read_csv(os.path.join(DATA_DIR, CLEAN_DATA_FILE))

    # Remove KOBO prefixes from column names.
    data = data.rename(utils.remove_prefix_list, axis='columns')

    # Cleanup Admin code data
    data['ADMIN1Code'] = data.apply(
        lambda x: utils.force_inital_zeros(x['ADMIN1Code'], 2),  axis=1)
    data['ADMIN2Code'] = data.apply(
        lambda x: utils.force_inital_zeros(x['ADMIN2Code'], 4),  axis=1)
    data['ADMIN3Code'] = data.apply(
        lambda x: utils.force_inital_zeros(x['ADMIN3Code'], 6),  axis=1)

    print('Loading SQL structure...')
    sql_schema = pd.read_excel(
        SQL_SCHEMA_FILE, sheet_name=SQL_SCHEMA_SHEET_NAME, dtype=str)
    final_sql_columns = list(sql_schema['Clean table'][1:])

    print('Loading labels...')
    choice_labels = load_choice_labels()

    print('initializing empty dataset...')
    # Initialize empty table and tracking
    final_data = pd.DataFrame(columns=final_sql_columns)
    remaining_columns = []
    cm_columns = []

    # print(final_sql_columns)

    for i, column in enumerate(final_sql_columns):
        if (column in data.columns):
            print(column + ' is a data column')
            final_data[column] = data[column]
            if (column in data.columns) and (column in CM.keys()):
                print(
                    column, 'This is a multiple choice column, populating expanded data...')
                CM_parameters = CM[column]
                for name_dict in choice_labels[CM_parameters['label_key']]:
                    name = name_dict.keys()[0]
                    column_name = CM_parameters['column_name'] + name

                    # New column has 0 or 1 if value was present
                    final_data[column_name] = final_data.apply(
                        lambda x: (str(name) in str(x[column])),
                        axis=1, result_type='expand'
                    )
                    # Label data always has the label available
                    final_data[column_name + '_2'] = name_dict[name]

                    # Add columns to processed
                    cm_columns.append(column_name)
                    cm_columns.append(column_name + '_2')

            continue

        elif is_label(column, final_sql_columns, data.columns):
            # TODO Add real label
            print(column + ' label')
            # final_data[column] = column + ' label'
            # Account for different naming conventions
            data_column = ''
            if column[:-1] in data.columns:
                data_column = column[:-1]
            elif column[:-2] in data.columns:
                data_column = column[:-2]
            else:
                raise('No matching data column for ' + column)

            # print(data_column)

            try:
                final_data[column] = final_data.apply(
                    lambda x: get_label(
                        list_name=get_sc_list_name(data_column),
                        name=x[data_column],
                        choice_labels=choice_labels),
                    axis=1, result_type='expand'
                )
            except Exception as e:
                print('/n')
                print('column')
                print(e)

            continue

        elif column in cm_columns:
            print(column, 'populated with CM columns')
            continue

        elif is_admin_label(column):
            admin_number = str(is_admin_label(column)[0])
            data_column_name = 'ADMIN' + admin_number + 'Code'
            label_column_name = 'ADM' + admin_number + 'Code'

            final_data[column] = final_data.apply(
                lambda x: get_label(
                    label_column_name, name=x[data_column_name], choice_labels=choice_labels),
                axis=1, result_type='expand'
            )
            continue

        print(column, 'does not fit a category yet')
        remaining_columns.append(column)

    print('Remaining columns')
    for col in remaining_columns:
        print(col)

    print(final_data.head())
    # print((column in data.columns), remaining_columns)

    # load clean data
    # initialize
    # only keep columns
    # load clean columns
    # load labels
    # possible prefix CM.

    # print('Process complete! clean SQL tables are now available under ./data.')


if __name__ == '__main__':
    main()
