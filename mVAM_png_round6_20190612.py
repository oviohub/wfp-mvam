import requests
import pandas as pd
import os

# Change these values to your own directory
DATA_DIR = './data'
ADMIN_AREA_FILE = '.data/Sampling frame_UNWFP May 2019_GeoCode.xlsx'

# File names
RAW_DATA_FILE = 'png_round6_raw_data.csv'
CLEAN_DATA_FILE = 'png_round6_clean_data.csv'
ENUMERATOR_FILE = 'png_round6_survey_by_enumerator.csv'
DUPLICATES_FILE = 'png_round6_duplicates.csv'
SURVEY_TARGETS = 'png_round6_survey_targets.csv'


# fetches data from Kobo
def download_data(survey_name, save=True, verbose=False):
    token = os.environ.get('TOKEN')
    if not token:
        raise Exception('Missing TOKEN.')

    response = requests.get(
        'https://kc.humanitarianresponse.info/api/v1/forms',
        headers={'Authorization': 'Token {token}'.format(token=token)})

    # Raise an error if the reponse status is not 200.
    response.raise_for_status()

    formid = None
    for item in response.json():
        # print(item, item)
        # print(item['title'], item['formid'])
        if item.get('title', None) == survey_name:
            print "Matched survey {0} with {1}, form id {2}".format(item['title'], survey_name, item['formid'])
            formid = item['formid']
    raw_data = None
    if formid:
        form_data = requests.get('https://kc.humanitarianresponse.info/api/v1/data/{0}'.format(formid),
                                 headers={'Authorization': 'Token {token}'.format(token=token)})

        # Raise an error if the reponse status is not 200.
        response.raise_for_status()

        raw_data = pd.read_json(form_data.content)

        # Save raw form data to analyze
        # with open(os.path.join(DATA_DIR, 'form_data_content.txt'), 'w') as f:
        #     f.write(form_data.content)

        raw_data.rename(columns={'': 'survey.idx'}, inplace=True)
        if save:
            raw_data.to_csv(os.path.join(DATA_DIR, RAW_DATA_FILE))
        if verbose:
            print raw_data.head()
            print raw_data.shape

    return raw_data


def clean_data(raw_df, save=True):
    raw_df[["RESPConsent", "Complete"]] = raw_df[[
        "RESPConsent", "Complete"]].apply(pd.to_numeric)
    # valid if respondent agreed to participate and survey form is marked complete
    valid_df = raw_df[(raw_df['RESPConsent'] < 3) & (raw_df['Complete'] == 1)]

    # check for duplicate respondent ids, and only keep first complete survey (by end time)
    duplicates = valid_df[['SvyDate', 'RESPId']]
    duplicates = duplicates[duplicates.duplicated(['RESPId'], keep=False)]
    duplicates = duplicates.pivot_table(index='RESPId', aggfunc='size')
    if save:
        duplicates.to_csv(os.path.join(
            DATA_DIR, DUPLICATES_FILE), header=['Count'])
    valid_df.sort_values(by='end')
    valid_df.drop_duplicates('RESPId', keep='first', inplace=True)

    # export summary of completed surveys per enumerator
    enumerator_crosstab = pd.crosstab(
        valid_df['EnuName'], valid_df['Complete'])
    enumerator_crosstab.columns = ['Completed']
    enumerator_crosstab.loc['Total'] = pd.Series(
        enumerator_crosstab['Completed'].sum(), index=['Completed'])
    if save:
        enumerator_crosstab.to_csv(os.path.join(DATA_DIR, ENUMERATOR_FILE))

    valid_df = clean_columns(valid_df)
    if save:
        valid_df.to_csv(os.path.join(DATA_DIR, CLEAN_DATA_FILE))

    return valid_df


def clean_columns(df):
    unwanted_cols = ['today', 'simserial', 'subscriberid', 'phonenumber', 'enu_note', 'RESPConsent',
                     'CallBackDate', 'CallBackHour', '_1_4_How_many_members_f_your_household_are',
                     'Error_The_total_num_respondent_to_verify', '_2_14_What_are_the_MA_rd_up_to_3_responses',
                     'Now_I_would_like_to_E_PAST_MONTH_30_DAYS', '_3_4_What_is_your_hou_rd_only_one_response',
                                     '_4_4_Currently_what_s_top_three_concerns']
    df.drop(unwanted_cols, axis=1, inplace=True)

    return df


def count_target_by_llg(df, save=True):
    # generate counts per LLG
    llg_count = df[df.duplicated(['ADMIN3Code'], keep=False)]
    llg_count = pd.DataFrame(df.pivot_table(
        index='ADMIN3Code', aggfunc='size'))
    llg_count.columns = ['Completed']

    # merge with llg data
    admin_areas = pd.read_excel(
        ADMIN_AREA_FILE, sheet_name='Master Sheet', dtype=str)
    df = pd.merge(admin_areas, llg_count, how='left', left_on=[
        'GEOCODE'], right_on=['ADMIN3Code'])

    # replace NaN values with 0, convert strings to ints
    df['Completed'].fillna(0, inplace=True)
    df['Completed'] = df['Completed'].astype(int)
    df['Target_sample'] = df['Target_sample'].astype(int)

    # get remaining counts, must not exceed 0
    df['Remaining'] = df['Target_sample'].sub(df['Completed'], axis=0)
    df['Remaining'] = df['Remaining'].clip(lower=0)
    df.drop(df.tail(1).index, inplace=True)
    df = df[['LLG', 'GEOCODE', 'Target_sample', 'Completed', 'Remaining']]
    if save:
        df.to_csv(os.path.join(DATA_DIR, SURVEY_TARGETS))

    return df


def main():
    pd.options.mode.chained_assignment = None
    # data = pd.read_excel('./PNG - mVAM REVVG - 3 June.xlsx', sheet_name='PNG - mVAM REVVG', dtype=str)
    data = download_data('PNG mVAM Round 6 (April 2019)')
    df = clean_data(data)
    count_target_by_llg(df)

    return


if __name__ == '__main__':
    main()
