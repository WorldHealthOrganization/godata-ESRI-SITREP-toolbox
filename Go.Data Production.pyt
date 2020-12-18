# -*- coding: utf-8 -*-

import arcpy
import requests
import os
import sys
import json
import csv
import collections
import copy
from datetime import datetime, timedelta
from pathlib import Path

def create_working_directory(in_loc):
    now_ts = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # current_wd = Path(os.path.dirname(os.path.realpath(__file__)))
    output_path = Path(in_loc)
    
    job_ws_foldername = f'GoData CSVs - {now_ts}'

    full_job_path = output_path.joinpath(job_ws_foldername)
    full_job_path.mkdir()

    return [full_job_path, now_ts]

def get_token(url, username, password):
    data = {
        "username": username,
        "password": password
    }

    token_res = None
    try:
        token_res = requests.post(f'{url}/api/oauth/token', data=data)
    except:
        return 'not_set'

    # return token_res.url
    token_res_json = token_res.json()

    token = 'not_set'
    if 'access_token' in token_res_json:
        token = token_res_json['access_token']
        return token
    elif 'error' in token_res_json:
        error = token_res_json['error']
        if 'message' in error:
            msg = error['message']
            status_code = error['statusCode']
            return f'Error {status_code} :: {msg}'
        else:
            return 'error autheticating'
    
    return token

def get_outbreaks(url, access_token):
    global outbreaks_cache

    params = { "access_token": access_token }
    available_outbreaks = []

    outbreaks_res = requests.get(f'{url}/api/outbreaks', params=params)
    outbreaks_res_json = outbreaks_res.json()
    if 'error' in outbreaks_res_json:
        message = outbreaks_res_json['error']['message']
        return message

    for outbreak in outbreaks_res_json:
        name = outbreak['name']
        id = outbreak['id']
        outbreaks_cache[name] = id
        available_outbreaks.append(name)

    return available_outbreaks

def get_ref_data(in_gd_api_url, token):
    params = {
        "type": "json",
        "access_token": token
    }
    ref_data = requests.get(f'{in_gd_api_url}/api/reference-data/export', params=params)
    ref_data_json = ref_data.json()
    arcpy.AddMessage(f'got reference data :: {len(ref_data_json)} items found')
    return ref_data_json

def get_cases(outbreak_id, in_gd_api_url, token):
    params = {
        "access_token": token,
        "filter": json.dumps({"where":{"and":[{"classification":{"neq":"LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_NOT_A_CASE_DISCARDED"}}],"countRelations":True},"include":[{"relation":"dateRangeLocations","scope":{"filterParent":False,"justFilter":False}},{"relation":"createdByUser","scope":{"filterParent":False,"justFilter":False}},{"relation":"updatedByUser","scope":{"filterParent":False,"justFilter":False}},{"relation":"locations","scope":{"filterParent":False,"justFilter":False}}],"limit":0,"skip":0})
    }
    case_data = requests.get(f'{in_gd_api_url}/api/outbreaks/{outbreak_id}/cases', params=params)
    case_data_json = case_data.json()
    arcpy.AddMessage(f'got cases data :: {len(case_data_json)} cases found')
    return case_data_json

def get_value_from_code(case_value, ref_data):
    for ref in ref_data:
        if ref['ID'] == case_value:
            return ref['Label']
    
    return case_value

def convert_features_to_csv(features):
    rows = []
    headers = features[0]['attributes'].keys()
    for f in features:
        row = {}
        for att in headers:
            row[att] = f['attributes'][att]
        
        rows.append(row)
    
    return headers, rows

def convert_gd_json_to_csv(cases, ref_data):
    features = []
    for case in cases:
        feature = {}
        keys = case.keys()
        for key in keys:
            if key == 'addresses':
                address = case[key][0]
                location_id = address['locationId']
                feature['locationId'] = location_id
                feature['locationClassification'] = address['typeId']
            elif key == 'age':
                feature['age_years'] = case[key]['years']
                feature['age_months'] = case[key]['months']
            elif not isinstance(case[key], collections.Mapping) and not isinstance(case[key], list):
                case_value = case[key]
                if isinstance(case_value, str) and 'LNG_' in case_value:
                    feature[f'{key}_code'] = case_value
                    case_value = get_value_from_code(case_value, ref_data)
                
                feature[key] = case_value
                
        features.append(feature)

    return features

def create_csv_file(rows, file_name='fromGoData.csv', field_names=None, full_job_path=None):
    full_path_to_file = full_job_path.joinpath(file_name)

    with open(full_path_to_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()

        for row in rows:
            writer.writerow(row)
    
    return full_path_to_file.resolve()

def get_attribute_model(in_model):
    attribute_models = {
        'cases_by_reporting_area':  {
            'attributes': { 
                'locationId': None,
                'DAILY_NEW_CONFIRMED_PROBABLE': None,
                'CUM_CONFIRMED_PROBABLE': None,
                'SUSPECT_LAST_SEVEN': None,
                'CONFIRMED_LAST_SEVEN': None,
                'PROBABLE_LAST_SEVEN': None,
                'TOTAL_CONFIRMED_PROBABLE_LAST_SEVEN': None,
                'CONFIRMED_LAST_FOURTEEN': None,
                'PROBABLE_LAST_FOURTEEN': None,
                'TOTAL_CONFIRMED_PROBABLE_LAST_FOURTEEN': None
            }
        },
        'deaths_by_reporting_area':  {
            'attributes': { 
                'locationId': None,
                'CUM_DEATHS': None,
                'DEATHS_LAST_SEVEN': None,
                'DEATHS_LAST_FOURTEEN': None
            }
        },
        'pctchg_by_reporting_area': {
            'attributes': {
                'locationId': None,
                'AVG_CONFIRMED_PROBABLE_LAST_SEVEN': None,
                'AVG_CONFIRMED_PROBABLE_LAST_FOURTEEN': None,
                'AVG_CONFIRMED_PROBABLE_EIGHT_TO_FOURTEEN': None,
                'AVG_CONFIRMED_PROBABLE_FIFTEEN_TO_TWENTY_EIGHT': None,
                'PERCENT_CHANGE_RECENT_SEVEN': None,
                'PERCENT_CHANGE_RECENT_FOURTEEN': None
            }
        }
    }

    return attribute_models[in_model]

def get_feature(location_id, features, model_id):
    for x in features:
        if x['attributes']['locationId'] == location_id:
            return x
    
    feature = copy.deepcopy(get_attribute_model(model_id))
    feature['attributes']['locationId'] = location_id
    
    features.append(feature)
    return feature

def increment_count(feature, field):
    current_count = feature['attributes'][field]
    if current_count is None:
        feature['attributes'][field] = 1
    else:
        feature['attributes'][field] = current_count + 1

    return feature['attributes'][field]

def get_geom(geo_field, geo_value, join_field_type):
    global geom_cache
    global geo_fl

    # if join_field_type == 'TEXT':
    #     geo_value = f"'{geo_value}'"

    wc = """{0} = '{1}'""".format(arcpy.AddFieldDelimiters(geo_fl, geo_field), geo_value)
    # arcpy.AddMessage(wc)
    if geo_value in geom_cache:
        # arcpy.AddMessage(f'got from cache for {geo_value}')
        return geom_cache[geo_value]
    else:
        geom = None
        row = None
        try:
            row = next(arcpy.da.SearchCursor(geo_fl, ['SHAPE@', geo_field],where_clause=wc))
        except: 
            pass

        if row and len(row) > 0:
            # arcpy.AddMessage(row)
            geom = row[0]
            geo_val_to_add = row[1]
            geom_cache[geo_val_to_add] = geom
        else:
            arcpy.AddMessage(f'Unable to get geometry from Geography layer. The where_clause, {wc} did not return results.')

        return geom

def create_fc_table(path_to_csv_file, in_gd_outworkspace, output_filename):
    try:
        arcpy.TableToTable_conversion(path_to_csv_file, in_gd_outworkspace, output_filename)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error

def create_featureclass(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, unique_location_ids):
    global geom_cache
    geom_cache = {}

    global geo_fl
    geo_fl = 'geo_fl'

    final_output_fc_path = os.path.join(in_gd_outworkspace, output_filename)

    arcpy.SetProgressor('default', 'Converting CSV file to Table in memory ...')
    # write csv to temp table in output workspace - will be deleted later
    tmp_cases_table = 'tbl_tmp'
    try:
        arcpy.TableToTable_conversion(str(path_to_csv_file), 'memory', tmp_cases_table)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error, None

    in_mem_gd_tbl = f'memory\\{tmp_cases_table}'

    try:
        arcpy.MakeFeatureLayer_management(in_gd_geolayer, geo_fl)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error, None

    in_geo_fl_desc = arcpy.Describe(geo_fl)
    in_geo_field_info = in_geo_fl_desc.fieldInfo
    geo_layer_feature_type = in_geo_fl_desc.shapeType
    geo_layer_sr = in_geo_fl_desc.spatialReference

    arcpy.SetProgressor('default', f'Creating {output_filename} feature class ...')
    try:
        arcpy.CreateFeatureclass_management(in_gd_outworkspace, output_filename, geo_layer_feature_type, '#', '#', '#', geo_layer_sr)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error, None

    arcpy.SetProgressor('default', 'Building fields to add to output feature class ...')
    # build list of fields to add
    add_field_type_map = {
        'Integer': 'LONG',
        'String': 'TEXT',
        'SmallInteger': 'SHORT'
    }

    gd_tbl_fields = []
    join_field_type = 'text'
    gd_fields = arcpy.ListFields(in_mem_gd_tbl)
    for f in gd_fields:
        if not f.required:
            alias = f.aliasName            
            field_type = f.type
            if f.type in add_field_type_map.keys():
                field_type = add_field_type_map[f.type]

            gd_tbl_fields.append([f.name, field_type, alias, f.length])

    # add the fields
    arcpy.SetProgressor('default', 'Adding fields to output feature class ...')
    try:
        arcpy.AddFields_management(os.path.join(in_gd_outworkspace, output_filename), gd_tbl_fields)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error, None

    gd_fields_list = [f.name for f in gd_fields]
    gd_fields_list.insert(0, 'SHAPE@')

    cnt = int(arcpy.GetCount_management(in_mem_gd_tbl)[0])
    arcpy.SetProgressor('step', f'Inserting {cnt} rows into output feature class ...', 0, cnt, 1)
    # add features with geometry to the output feature class
    counter = 1
    with arcpy.da.SearchCursor(in_mem_gd_tbl, '*') as cursor:
        for row in cursor:
            arcpy.SetProgressorPosition(counter)
            arcpy.SetProgressorLabel(f'Inserting row {counter} of {cnt} ...')

            gd_cursor_fields = cursor.fields
            search_val = row[gd_cursor_fields.index('locationId')]
            geom = get_geom(in_gd_geojoinfield, search_val, join_field_type)

            row_list = list(row)
            row_list.insert(0, geom)
            insert_row = tuple(row_list)
            # arcpy.AddMessage(insert_row)

            with arcpy.da.InsertCursor(final_output_fc_path, gd_fields_list) as ic:
                try:
                    ic.insertRow(insert_row)
                    counter = counter + 1
                except:
                    arcpy.AddMessage(ic.fields)
                    arcpy.AddError('Error inserting rows')
                    return 'Error inserting rows', None

    if in_gd_shouldkeepallgeo:
        arcpy.ResetProgressor()
        arcpy.SetProgressor('default', f'Inserting unmatched geographies into output feature class ...')
    
        # arcpy.AddMessage(unique_location_ids)
        joined = "','".join(unique_location_ids)
        wc = f"{in_gd_geojoinfield} NOT IN ('{joined}')"
        # arcpy.AddMessage(wc)
        with arcpy.da.SearchCursor(geo_fl, ['SHAPE@', in_gd_geojoinfield], where_clause=wc) as cursor:
            for row in cursor:
                geom = row[0]
                geo_val_to_add = row[1]
                insert_row = (geom, geo_val_to_add)
                with arcpy.da.InsertCursor(final_output_fc_path, ['SHAPE@', 'locationId']) as ic:
                    try:
                        ic.insertRow(insert_row)
                        counter = counter + 1
                    except:
                        arcpy.AddMessage(ic.fields)
                        arcpy.AddError('Error inserting rows')
                        return 'Error inserting rows', None

    arcpy.ResetProgressor()

    # delete the in memory workspace
    arcpy.SetProgressor('default', 'Cleaning up temporary files ...')
    arcpy.Delete_management(in_mem_gd_tbl)

    # clean up geometry cache variable
    del geom_cache

    return None, final_output_fc_path    


def join_to_geo(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, tbl_name, unique_location_ids):
    # create cases FC table
    arcpy.SetProgressor('default', f'Joining {tbl_name} table to geography ...')
    err, fc_path = create_featureclass(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, unique_location_ids)
    
    return err, fc_path

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Create SITREP Tables"
        self.description = "Generate summary tables for SITREP templates"

        # List of tool classes associated with this toolbox
        self.tools = [CreateSITREPTables]

class CreateSITREPTables(object):

    global outbreaks_cache 
    outbreaks_cache = {}

    global selected_outbreak_id
    selected_outbreak_id = None

    global token
    token = None

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create SITREP Tables"
        self.description = "Generate summary tables for SITREP templates"
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
         #Define parameter definitions

        #######################
        # START DEFINE PARAMS #
        #######################

        # Go.Data Url (API)
        param_url = arcpy.Parameter(
            displayName="Go.Data Url",
            name="in_url",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Username
        param_username = arcpy.Parameter(
            displayName="Username",
            name="in_username",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        # Password
        param_password = arcpy.Parameter(
            displayName="Password",
            name="in_password",
            datatype="GPStringHidden",
            parameterType="Required",
            direction="Input")

        # Outbreaks
        param_outbreak = arcpy.Parameter(
            displayName="Outbreak",
            name="in_outbreak",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Output Folder for CSV files
        param_output_folder = arcpy.Parameter(
            displayName="Output folder for CSVs",
            name="in_outcsvfolder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        # Output Workspace
        param_outworkspace = arcpy.Parameter(
            displayName="Output Workspace",
            name="in_outputfcworkspace",
            datatype="DEWorkspace",
            parameterType="Optional",
            direction="Input")

        # Join to Geography
        param_joingeo = arcpy.Parameter(
            displayName="Join to Geography",
            name="in_joingeo",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        # Geography Layer
        param_geolayer = arcpy.Parameter(
            displayName="Geography Layer",
            name="in_geolayer",
            datatype=["GPFeatureLayer", "GPLayer", "Shapefile"],
            parameterType="Optional",
            direction="Input")

        # Geography Join Field
        param_geojoinfield = arcpy.Parameter(
            displayName="Geography Join Field",
            name="in_geofield",
            datatype="Field",
            parameterType="Optional",
            direction="Input")

        # Keep all Geography Features
        param_keepallgeo = arcpy.Parameter(
            displayName="Keep all Geography Features",
            name="in_keepallgeo",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        # output feature classes -- TODO figure this out to return multiple feature classes
        # https://gis.stackexchange.com/questions/9406/using-multivalue-output-parameter-with-arcpy
        param_outputfcpaths = arcpy.Parameter(
            displayName="Output Features",
            name="param_outputfcpath",
            datatype="GPFeatureLayer",
            multiValue=True,
            parameterType="Derived",
            direction="Output")

        param_geojoinfield.parameterDependencies = [param_geolayer.name]

        param_geolayer.enabled = False
        param_geojoinfield.enabled = False
        param_keepallgeo.enabled = False

        return [param_url, param_username, param_password, param_outbreak, param_output_folder, param_outworkspace, param_joingeo, param_geolayer, param_geojoinfield, param_keepallgeo, param_outputfcpaths]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        global outbreaks_cache
        global selected_outbreak_id
        global token

        if parameters[0].altered and not parameters[0].hasBeenValidated or (parameters[1].altered and not parameters[1].hasBeenValidated) or (parameters[2].altered and not parameters[2].hasBeenValidated):
            url = parameters[0].value
            username = parameters[1].value
            password = parameters[2].value

            if not parameters[0].value or not parameters[1].value or not parameters[2].value:
                return

            token = get_token(url, username, password)

            if token == 'not_set':
                return 

            parameters[3].filter.list = []
            outbreaks = get_outbreaks(url, token)
            if not isinstance(outbreaks, list):
                parameters[3].value = outbreaks
                return

            parameters[3].filter.list = outbreaks
            parameters[3].value = outbreaks[0]
         

        if parameters[3].value:
            # parameters[10].value = outbreaks_cache[parameters[3].value]
            selected_outbreak_id = outbreaks_cache[parameters[3].value]


        parameters[7].enabled = parameters[6].value
        parameters[8].enabled = parameters[6].value
        parameters[9].enabled = parameters[6].value
       
        return

    def updateMessages(self, parameters):

        return

    def execute(self, parameters, messages):
         # setup globals
        global selected_outbreak_id
        global token

        ################
        # SCRIPT START #
        ################

        # Collect parameters
        in_gd_api_url = parameters[0].valueAsText
        in_gd_username = parameters[1].valueAsText
        in_gd_password = parameters[2].valueAsText
        in_gd_outcsvfolder = parameters[4].valueAsText
        in_gd_outworkspace = parameters[5].valueAsText
        in_gd_shouldjoin = parameters[6].value    
        in_gd_geolayer = parameters[7].value
        in_gd_geojoinfield = parameters[8].valueAsText
        in_gd_shouldkeepallgeo = parameters[9].value

        wd_res = create_working_directory(in_gd_outcsvfolder)
        full_job_path = wd_res[0]
        now_ts = wd_res[1]
        output_paths = []  

        in_gd_outbreak = selected_outbreak_id

        # get reference codes & labels
        # e.g. LNG_REFERENCE_DATA_CATEGORY_OUTCOME_ALIVE = 'Alive'
        arcpy.SetProgressor('default', 'Getting Go.Data reference data')
        ref_data = get_ref_data(in_gd_api_url, token)

        # get outbreak cases
        arcpy.SetProgressor('default', 'Getting Outbreak Cases')
        cases = get_cases(selected_outbreak_id, in_gd_api_url, token)
        new_cases = convert_gd_json_to_csv(cases, ref_data)

        # setup needed dates
        dte_format = '%Y-%m-%dT%H:%M:%S.%fZ'

        right_now = datetime.now()
        yesterday_delta = timedelta(days=1)
        eight_days_delta = timedelta(days=8)
        fifteen_days_delta = timedelta(days=15)
        twenty_eight_days_delta = timedelta(days=28)
        one_week_delta = timedelta(weeks=1)
        two_week_delta = timedelta(weeks=2)

        yesterday = (right_now - yesterday_delta).date()
        eight_days_ago = (right_now - eight_days_delta).date()
        fifteen_days_ago = (right_now - fifteen_days_delta).date()
        twenty_eight_days_ago = (right_now - twenty_eight_days_delta).date()
        yesterday = (right_now - yesterday_delta).date()
        last_week = (right_now - one_week_delta).date()
        last_two_weeks = (right_now - two_week_delta).date()

        start_date = min(list([datetime.strptime(c['dateOfReporting'], dte_format).date() for c in new_cases]))

        # Cases by Reporting Area
        features = []
        for case in new_cases:
            location_id = case['locationId']
    
            reporting_date = datetime.strptime(case['dateOfReporting'], dte_format).date()
    
            feature = get_feature(location_id, features, 'cases_by_reporting_area')
    
            # yesterday
            if reporting_date == yesterday:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'DAILY_NEW_CONFIRMED_PROBABLE')
   
    
            if reporting_date >= start_date:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'CUM_CONFIRMED_PROBABLE')                    
    
            if reporting_date >= last_week:
                conf = None
                prob = None
        
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                    cnt = increment_count(feature, 'CONFIRMED_LAST_SEVEN')
                    conf = cnt
                elif case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    cnt = increment_count(feature, 'PROBABLE_LAST_SEVEN')
                    prob = cnt

                elif case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_SUSPECT':
                    increment_count(feature, 'SUSPECT_LAST_SEVEN')
        
                if prob is not None or conf is not None:
                    if prob is None:
                        prob = 0
                    if conf is None:
                        conf = 0
                
                    current_count = feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_SEVEN']
                    if current_count is None:
                        current_count = 0
                        feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_SEVEN'] = 0
            
                    feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_SEVEN'] = current_count + (prob + conf)

            if reporting_date >= last_two_weeks:
                conf = None
                prob = None
        
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                    cnt = increment_count(feature, 'CONFIRMED_LAST_FOURTEEN')
                    conf = cnt
                elif case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    cnt = increment_count(feature, 'PROBABLE_LAST_FOURTEEN')
                    prob = cnt
            
                if prob is not None or conf is not None:
                    if prob is None:
                        prob = 0
                    if conf is None:
                        conf = 0
                
                    current_count = feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_FOURTEEN']
                    if current_count is None:
                        current_count = 0
                        feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_FOURTEEN'] = 0
            
                    feature['attributes']['TOTAL_CONFIRMED_PROBABLE_LAST_FOURTEEN'] = current_count + (prob + conf)

        # convert features back to csv_rows
        headers, cases_by_rep_csv_rows = convert_features_to_csv(features)

        # create cases CSV
        arcpy.SetProgressor('default', 'Creating Cases CSV file ...')
        path_to_csv_file = create_csv_file(cases_by_rep_csv_rows, 'Cases_by_Reporting_Area.csv', headers, full_job_path)

        # create cases FC table
        arcpy.SetProgressor('default', 'Creating Cases feature class table ...')
        output_filename = 'Cases_By_Reporting_Area'
        err = create_fc_table(str(path_to_csv_file), in_gd_outworkspace, output_filename)
        if err is not None:
            arcpy.AddError(error)

        unique_location_ids = []
        for c in new_cases:
            for k in c.keys():
                if k == 'locationId':
                    if not c[k] in unique_location_ids:
                        unique_location_ids.append(c[k])

        if in_gd_shouldjoin:
            # create cases FC table
            err, fc_path = join_to_geo(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Cases by Reporting Area', unique_location_ids)
            if err is not None:
                arcpy.AddError(err)

            output_paths.append(fc_path)           
             

        # Deaths by Reporting Area
        filtered_cases = [c for c in new_cases if 'outcomeId_code' in c and c['outcomeId_code'] == 'LNG_REFERENCE_DATA_CATEGORY_OUTCOME_DECEASED']

        unique_location_ids = []
        for c in filtered_cases:
            for k in c.keys():
                if k == 'locationId':
                    if not c[k] in unique_location_ids:
                        unique_location_ids.append(c[k])

        deaths_features = []
        for case in filtered_cases:
            location_id = case['locationId']
    
            reporting_date = datetime.strptime(case['dateOfReporting'], dte_format).date()
            
            feature = get_feature(location_id, deaths_features, 'deaths_by_reporting_area') 
    
            if reporting_date >= start_date:
                increment_count(feature, 'CUM_DEATHS')                    
    
            if reporting_date >= last_week:
                increment_count(feature, 'DEATHS_LAST_SEVEN')

            if reporting_date >= last_two_weeks:
                increment_count(feature, 'DEATHS_LAST_FOURTEEN')


        # convert back to csv
        headers, deaths_by_rep_csv_rows = convert_features_to_csv(deaths_features)

        # create deaths CSV
        arcpy.SetProgressor('default', 'Creating Deaths by Reporting Area CSV file ...')
        path_to_csv_file = create_csv_file(deaths_by_rep_csv_rows, 'Deaths_by_Reporting_Area.csv', headers, full_job_path)

        # create deaths FC table
        arcpy.SetProgressor('default', 'Creating Deaths by Reporting Area feature class table ...')
        output_filename = 'Deaths_By_Reporting_Area'
        err = create_fc_table(str(path_to_csv_file), in_gd_outworkspace, output_filename)
        if err is not None:
            arcpy.AddError(error)

        # join to geography
        if in_gd_shouldjoin:
            err, fc_path = join_to_geo(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Deaths by Reporting Area', unique_location_ids)
            if err is not None:
                arcpy.AddError(err)

            output_paths.append(fc_path)


        # Percent Change in New Cases by Reporting Area
        unique_location_ids = []
        for c in new_cases:
            for k in c.keys():
                if k == 'locationId':
                    if not c[k] in unique_location_ids:
                        unique_location_ids.append(c[k])

        pctchg_features = []
        sum_conf_prob_7 = 0
        sum_conf_prob_14 = 0
        sum_conf_prob_8_14 = 0
        sum_conf_prob_15_28 = 0

        for case in new_cases:
            location_id = case['locationId']
    
            reporting_date = datetime.strptime(case['dateOfReporting'], dte_format).date()
            reporting_date_fm = reporting_date.strftime('%Y-%m-%d')
        
            feature = get_feature(location_id, pctchg_features, 'pctchg_by_reporting_area')                   
    
            if reporting_date >= last_week:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'AVG_CONFIRMED_PROBABLE_LAST_SEVEN')
                
            if reporting_date >= last_two_weeks:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'AVG_CONFIRMED_PROBABLE_LAST_FOURTEEN')
            
            if eight_days_ago >= reporting_date >= last_two_weeks:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'AVG_CONFIRMED_PROBABLE_EIGHT_TO_FOURTEEN')
                        
            if fifteen_days_ago >= reporting_date >= twenty_eight_days_ago:
                if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED' or case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_PROBABLE':
                    increment_count(feature, 'AVG_CONFIRMED_PROBABLE_FIFTEEN_TO_TWENTY_EIGHT')

        for feature in pctchg_features:
            val = feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_SEVEN']
            if val is not None:
                feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_SEVEN'] = round(val / 7, 2)
        
            val = feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_FOURTEEN']
            if val is not None:
                feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_FOURTEEN'] = round(val / 14, 2)
    
            val = feature['attributes']['AVG_CONFIRMED_PROBABLE_EIGHT_TO_FOURTEEN']
            if val is not None:
                feature['attributes']['AVG_CONFIRMED_PROBABLE_EIGHT_TO_FOURTEEN'] = round(val / 7, 2)
        
            val = feature['attributes']['AVG_CONFIRMED_PROBABLE_FIFTEEN_TO_TWENTY_EIGHT']
            if val is not None:
                feature['attributes']['AVG_CONFIRMED_PROBABLE_FIFTEEN_TO_TWENTY_EIGHT'] = round(val / 14, 2)
        
            val2 = feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_SEVEN']
            val1 = feature['attributes']['AVG_CONFIRMED_PROBABLE_EIGHT_TO_FOURTEEN']
            if val2 is not None and val1 is not None and val2 > 0 and val1 > 0:
                pct_chg = ((val2 - val1) / abs(val1)) * 100
                feature['attributes']['PERCENT_CHANGE_RECENT_SEVEN'] = pct_chg
        
            val2 = feature['attributes']['AVG_CONFIRMED_PROBABLE_LAST_FOURTEEN']
            val1 = feature['attributes']['AVG_CONFIRMED_PROBABLE_FIFTEEN_TO_TWENTY_EIGHT']
            if val2 is not None and val1 is not None and val2 > 0 and val1 > 0:
                pct_chg = ((val2 - val1) / abs(val1)) * 100
                feature['attributes']['PERCENT_CHANGE_RECENT_FOURTEEN'] = pct_chg
    
        # convert back to csv
        headers, pct_by_rep_csv_rows = convert_features_to_csv(pctchg_features)

        # create pct change CSV
        arcpy.SetProgressor('default', 'Creating Percent Change in New Cases by Reporting Area CSV file ...')
        path_to_csv_file = create_csv_file(pct_by_rep_csv_rows, 'Percent_Change_in_New_Cases_by_Reporting_Area.csv', headers, full_job_path)

        # create pct change FC table
        arcpy.SetProgressor('default', 'Creating Percent Change in New Cases by Reporting Area feature class table ...')
        output_filename = 'Percent_Change_in_New_Cases_by_Reporting_Area'
        err = create_fc_table(str(path_to_csv_file), in_gd_outworkspace, output_filename)
        if err is not None:
            arcpy.AddError(error)

        # join to geography
        if in_gd_shouldjoin:
            err, fc_path = join_to_geo(path_to_csv_file, in_gd_outworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Percent Change in New Cases by Reporting Area', unique_location_ids)
            if err is not None:
                arcpy.AddError(err)

            output_paths.append(fc_path)


        if len(output_paths) > 0:
            # set the output parameter
            arcpy.SetParameter(10, ';'.join(output_paths))
        
        return 
