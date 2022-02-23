# -*- coding: utf-8 -*-

#  
# Copyright (C) Go.Data, World Health Organization GIS Centre for Health
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
#

__author__ = "@LangsterGA, @apfister, @adm-gis"
__copyright__ = "Copyright (C) 2022 Go.Data; World Health Organization GIS Centre for Health"
__license__ = "GNU General Public License"
__version__ = "1.0"



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
import pandas as pd

def set_working_directory(in_loc):
    now_ts = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    output_path = Path(in_loc) 
    return output_path

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
            return 'error authenticating'
    
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
    ref_data = requests.get(f'{in_gd_api_url}/api/reference-data', params=params)
    ref_data_json = ref_data.json()
    arcpy.AddMessage(f'got reference data :: {len(ref_data_json)} items found')
    return ref_data_json

def get_cases(outbreak_id, in_gd_api_url, token):
    params = {
        "access_token": token,
        #"filter": json.dumps({"where":{"and":[{"classification":{"neq":"LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_NOT_A_CASE_DISCARDED"}}],"countRelations":True},"include":[{"relation":"dateRangeLocations","scope":{"filterParent":False,"justFilter":False}},{"relation":"createdByUser","scope":{"filterParent":False,"justFilter":False}},{"relation":"updatedByUser","scope":{"filterParent":False,"justFilter":False}},{"relation":"locations","scope":{"filterParent":False,"justFilter":False}}],"limit":0,"skip":0})
    }
    case_data = requests.get(f'{in_gd_api_url}/api/outbreaks/{outbreak_id}/cases', params=params)
    case_data_json = case_data.json()
    arcpy.AddMessage(f'got cases data :: {len(case_data_json)} cases found')
    return case_data_json

def get_locations(in_gd_api_url, token):
    params = {
        "access_token": token,
    }
    location_data = requests.get(f'{in_gd_api_url}/api/locations', params=params)
    location_data_json = location_data.json()
    arcpy.AddMessage(f'got locations data :: {len(location_data_json)} locations found')
    return location_data_json

def get_contacts(outbreak_id, in_gd_api_url, token):
    params = {
        "access_token": token,
    }
    contact_data = requests.get(f'{in_gd_api_url}/api/outbreaks/{outbreak_id}/contacts', params=params)
    contact_data_json = contact_data.json()
    arcpy.AddMessage(f'got contacts data :: {len(contact_data_json)} contacts found')
    return contact_data_json

def get_relationships(outbreak_id, in_gd_api_url, token):
    params = {
        "access_token": token,
    }
    relate_data = requests.get(f'{in_gd_api_url}/api/outbreaks/{outbreak_id}/relationships', params=params)
    relate_data_json = relate_data.json()
    arcpy.AddMessage(f'got relationships data :: {len(relate_data_json)} relationships found')
    return relate_data_json

def get_followups(outbreak_id, in_gd_api_url, token):
    params = {
        "access_token": token,
    }
    followup_data = requests.get(f'{in_gd_api_url}/api/outbreaks/{outbreak_id}/follow-ups', params=params)
    followup_data_json = followup_data.json()
    arcpy.AddMessage(f'got followups data :: {len(followup_data_json)} followups found')
    return followup_data_json


def get_value_from_code(case_value, ref_data):
    for ref in ref_data:
        if ref['id'] == case_value:
            return ref['value']
    
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

def convert_cases_json_to_csv(cases, ref_data):
    features = []
    for case in cases:
        feature = {}
        keys = case.keys()
        for key in keys:
            if key == 'age':
                feature['age'] = case[key]['years']
            elif key == 'addresses':
                address = case[key][0]
                location_id = address['locationId']
                feature['locationId'] = location_id ## do not remove
                feature['locationClassification'] = address['typeId']
                if 'city' in address:
                    feature['city'] = address['city']
                if 'postalCode' in address:
                    feature['postalCode'] = address['postalCode']
                if 'addressLine1' in address:
                    feature['addressLine1'] = address['addressLine1']  
            elif key == 'locations':
                if len(case[key]) > 0:
                    location = case[key][0]
                    feature['adminLevel'] = location['geographicalLevelId'].split('_')[-1]
            elif key == 'dob':
                feature['dateOfBurial'] = case[key]
            elif key == 'vaccinesReceived':
                if len(case[key]) > 0:
                    feature['vaccinated'] = 'True'
                else:
                    feature['vaccinated'] = 'False'
            elif not isinstance(case[key], collections.abc.Mapping) and not isinstance(case[key], list):
                case_value = case[key]
                if isinstance(case_value, str) and 'LNG_' in case_value:
                    feature[f'{key}_code'] = case_value
                    case_value = get_value_from_code(case_value, ref_data)               
                feature[key] = case_value               
        features.append(feature)
    return features

def convert_loc_json_to_csv(locations, ref_data):
    features = []
    for loc in locations:
        feature = {}
        keys = loc.keys()
        for key in keys:
            if key == 'geoLocation':
                if loc[key] == None:
                    pass
                else:
                    feature['Lat'] = loc[key]['lat']
                    feature['Lng'] = loc[key]['lng']    
            elif not isinstance(loc[key], collections.abc.Mapping) and not isinstance(loc[key], list):
                loc_value = loc[key]
                if isinstance(loc_value, str) and 'LNG_' in loc_value:
                    feature[f'{key}_code'] = loc_value
                    loc_value = get_value_from_code(loc_value, ref_data)
                feature[key] = loc_value        
        features.append(feature)
    return features

def convert_contacts_json_to_csv(contacts, ref_data):
    features = []
    for contact in contacts:
        feature = {}
        keys = contact.keys()
        for key in keys:
            if key =='followUp':
                feature['dateFollowUpStart'] = contact[key]['startDate']
                feature['dateFollowUpEnd'] = contact[key]['endDate']
            elif key == 'age':
                feature['age'] = contact[key]['years']
            elif key == 'addresses':
                address = contact[key][0]
                location_id = address['locationId']
                feature['locationId'] = location_id
                feature['locationClassification'] = address['typeId']
                if 'city' in address:
                    feature['city'] = address['city']
                if 'postalCode' in address:
                    feature['postalCode'] = address['postalCode']
                if 'addressLine1' in address:
                    feature['addressLine1'] = address['addressLine1']  
                if 'emailAddress' in address:
                    feature['email'] = address['emailAddress']
                if 'phoneNumber' in address:
                    feature['phoneNumber'] = address['phoneNumber']
            elif key == 'vaccinesReceived':
                if len(contact[key]) > 0:
                    feature['vaccinated'] = 'True'
                else:
                    feature['vaccinated'] = 'False'
            elif key == 'dob':
                feature['dateOfBurial'] = contact[key]
            elif key == 'relationshipsRepresentation':
                feature['relationshipId'] = contact[key][0]['id']
            elif not isinstance(contact[key], collections.abc.Mapping) and not isinstance(contact[key], list):
                contact_value = contact[key]
                if isinstance(contact_value, str) and 'LNG_' in contact_value:
                    feature[f'{key}_code'] = contact_value
                    contact_value = get_value_from_code(contact_value, ref_data)
                feature[key] = contact_value
        features.append(feature)
    return features

def convert_relates_json_to_csv(relates, ref_data):
    features = []
    for relate in relates:
        feature = {}
        keys = relate.keys()
        for key in keys:
            if key == 'persons':
                for person in relate[key]:
                    if 'source' in list(person.keys()):
                        feature['source_person_id'] = person['id']
                        feature['source_person_type'] = person['type']
                    elif 'target' in list(person.keys()):
                        feature['target_person_id'] = person['id']
                        feature['target_person_type']= person['type']
            elif not isinstance(relate[key], collections.abc.Mapping) and not isinstance(relate[key], list):
                relate_value = relate[key]
                if isinstance(relate_value, str) and 'LNG_' in relate_value:
                    feature[f'{key}_code'] = relate_value
                    relate_value = get_value_from_code(relate_value, ref_data)
                feature[key] = relate_value
        features.append(feature)
    return features

def convert_followups_json_to_csv(followups, ref_data):
    features = []
    for followup in followups:
        feature = {}
        keys = followup.keys()
        for key in keys:
            if key == 'address':
                address = followup[key]
                feature['locationId'] = address['locationId']
                if 'city' in address:
                    feature['city'] = address['city']
                if 'postalCode' in address:
                    feature['postalCode'] = address['postalCode']
                if 'addressLine1' in address:
                    feature['addressLine1'] = address['addressLine1']  
                if 'emailAddress' in address:
                    feature['email'] = address['emailAddress']
                if 'phoneNumber' in address:
                    feature['phoneNumber'] = address['phoneNumber']
            elif key == 'contact':
                feature['visualId'] = followup[key]['visualId']
            elif not isinstance(followup[key], collections.abc.Mapping) and not isinstance(followup[key], list):
                followup_value = followup[key]
                if isinstance(followup_value, str) and 'LNG_' in followup_value:
                    feature[f'{key}_code'] = followup_value
                    followup_value = get_value_from_code(followup_value, ref_data)
                feature[key] = followup_value
        features.append(feature)
    return features

def create_csv_file(rows, file_name, field_names=None, full_job_path=None):
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
                'DAILY_NEW_CONFIRMED': None,
                'CUM_CONFIRMED': None,
                'CONFIRMED_LAST_SEVEN': None,
                'CONFIRMED_LAST_FOURTEEN': None,
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
                'AVG_CONFIRMED_LAST_SEVEN': None,
                'AVG_CONFIRMED_LAST_FOURTEEN': None,
                'AVG_CONFIRMED_EIGHT_TO_FOURTEEN': None,
                'AVG_CONFIRMED_FIFTEEN_TO_TWENTY_EIGHT': None,
                'PERCENT_CHANGE_RECENT_SEVEN': None,
                'PERCENT_CHANGE_RECENT_FOURTEEN': None
            }
        },
        'active_contacts_by_reporting_area': {
            'attributes': {
                'locationId': None,
                'locationName': None,
                'UNDER_FOLLOWUP': None,
                'PAST_7_DAYS': None,
                'PAST_14_DAYS': None,
            }
        }
    }
    return attribute_models[in_model]

def get_FieldNameUpdater(in_model):
    attribute_models = {
        'case_level_data': {
            'attributes': {
                'id':'id',
                'visualId':'visual_id',
                'numberOfContacts':'no_contacts',
                'numberOfExposures':'no_exposures',
                'classification':'classification',
                'firstName':'first_name',
                'middleName':'middle_name',
                'lastName':'last_name',
                'gender':'gender',
                'age':'age',
                'ageClass':'age_class',
                'occupation':'occupation',
                'pregnancyStatus':'pregnancy_status',
                'dateOfReporting':'date_of_reporting',
                'dateOfOnset':'date_of_onset',
                'dateOfInfection':'date_of_infection',
                'dateBecomeCase':'date_become_case',
                'dateOfBurial':'date_of_burial',
                'wasContact':'was_contact',
                'riskLevel':'risk_level',
                'riskReason':'risk_reason',
                'safeBurial':'safe_burial',
                'transferRefused':'transfer_refused',
                'responsibleUserId':'responsible_user_id',
                'admin_0_name':'admin_0_name',
                'admin_1_name':'admin_1_name',
                'admin_2_name':'admin_2_name',
                'admin_3_name':'admin_3_name',
                'admin_4_name':'admin_4_name',
                'Lat':'lat',
                'Lng':'long',
                'addressLine1':'address',
                'postalCode':'postal_code',
                'city':'city',
                'vaccinated':'vaccinated',
                'outcomeId':'outcome',
                'dateOfOutcome':'date_of_outcome',
                'locationId':'location_id',
                'createdBy':'created_by',
                'createdAt':'datetime_created_at',
                'updatedBy':'updated_by',
                'updatedAt':'datetime_updated_at',
            }
        },
         'contact_data': {
            'attributes': {
                'id':'id',
                'visualId':'visual_id',
                'classification':'classification',
                'firstName':'first_name',
                'middleName':'middle_name',
                'lastName':'last_name',
                'gender':'gender',
                'age':'age',
                'ageClass':'age_class',
                'occupation':'occupation',
                'vaccinated':'vaccinated',
                'pregnancyStatus':'pregnancy_status',
                'dateOfReporting':'date_of_reporting',
                'dateOfLastContact':'date_of_last_contact',
                'dateOfBurial':'date_of_burial',
                'underFollowUp':'follow_up_status',
                'followUpPast7Days': '',
                'followUpPast14Days': '',
                'dateFollowUpStart':'date_of_follow_up_start',
                'dateFollowUpEnd':'date_of_follow_up_end',
                'wasCase':'was_case',
                'riskLevel':'risk_level',
                'riskReason':'risk_reason',
                'safeBurial':'safe_burial',
                'responsibleUserId':'responsible_user_id',
                'followUpTeamId':'follow_up_team_id',
                'locationId':'location_id',
                'admin_0_name':'admin_0_name',
                'admin_1_name':'admin_1_name',
                'admin_2_name':'admin_2_name',
                'admin_3_name':'admin_3_name',
                'admin_4_name':'admin_4_name',
                'Lat':'lat',
                'Lng':'long',
                'addressLine1':'address',
                'postalCode':'postal_code',
                'city':'city',
                'phoneNumber':'telephone',
                'email':'email',
                'createdBy':'created_by',
                'createdAt':'datetime_created_at',
                'updatedBy':'updated_by',
                'updatedAt':'datetime_updated_at'
        }
    },
        'followup_data': {
            'attributes': {
                'id':'id',
                'personId':'contact_id',
                'visualId':'contact_visual_id',
                'date':'date',
                'index':'follow_up_number',
                'statusId':'follow_up_status',
                'targeted':'targeted',
                'responsibleUserId':'responsible_user_id',
                'teamId': 'team_id',
                'locationId':'location_id',
                'admin_0_name':'admin_0_name',
                'admin_1_name':'admin_1_name',
                'admin_2_name':'admin_2_name',
                'admin_3_name':'admin_3_name',
                'admin_4_name':'admin_4_name',
                'Lat':'lat',
                'Lng':'long',
                'addressLine1':'address',
                'postalCode':'postal_code',
                'city':'city',
                'phoneNumber':'telephone',
                'email':'email',
                'createdBy':'created_by',
                'createdAt':'datetime_created_at',
                'updatedBy':'updated_by',
                'updatedAt':'datetime_updated_at'
        }
    },
        'relationship_data': {
            'attributes': {
                'id':'id',
                'source_person_id':'source_person_id',
                'source_person_visual_id':'source_person_visual_id',
                'target_person_id':'target_person_id',
                'target_person_visual_id':'target_person_visual_id',
                'source_person_type':'source_person_type',
                'target_person_type':'target_person_type',
                'createdBy':'created_by',
                'createdAt':'datetime_created_at',
                'updatedBy':'updated_by',
                'updatedAt':'datetime_updated_at'
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
    global geom_cache #dict
    global geo_fl  #geography feature layer
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

        if row and len(row) > 0: #if the row exists and has data
            # arcpy.AddMessage(row)
            geom = row[0]
            geo_val_to_add = row[1]
            geom_cache[geo_val_to_add] = geom
        else:
            arcpy.AddMessage(f'Unable to get geometry from Geography layer. The where_clause, {wc} did not return results.')

        return geom

def create_fc_table(path_to_csv_file, in_gd_outgdbworkspace, output_filename):
    try:
        arcpy.TableToTable_conversion(path_to_csv_file, in_gd_outgdbworkspace, output_filename)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error

def create_featureclass(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, unique_location_ids):
    global geom_cache
    geom_cache = {}

    global geo_fl
    geo_fl = 'geo_fl'

    final_output_fc_path = os.path.join(in_gd_outgdbworkspace, output_filename)

    arcpy.SetProgressor('default', 'Converting CSV file to Table in memory ...')
    # write csv to temp table in output workspace - will be deleted later
    tmp_table = 'tbl_tmp'
    try:
        arcpy.TableToTable_conversion(str(path_to_csv_file), 'memory', tmp_table)
    except Exception:
        e = sys.exc_info()[1]
        error = e.args[0]
        print (error)
        return error, None

    in_mem_gd_tbl = f'memory\\{tmp_table}'

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
        arcpy.CreateFeatureclass_management(in_gd_outgdbworkspace, output_filename, geo_layer_feature_type, '#', '#', '#', geo_layer_sr)
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
    
    # for f in arcpy.ListFields(geo_fl):
    #     if not f.required:
    #         alias = f.aliasName            
    #         field_type = f.type
    #         if f.type in add_field_type_map.keys():
    #             field_type = add_field_type_map[f.type]
    #         if f.name not in drop_keys:
    #             gd_tbl_fields.append([f.name, field_type, alias, f.length])


    arcpy.SetProgressor('default', 'Adding fields to output feature class ...')
    try:
        arcpy.AddFields_management(os.path.join(in_gd_outgdbworkspace, output_filename), gd_tbl_fields)
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

def join_to_geo(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, tbl_name, unique_location_ids):
    # create cases FC table
    arcpy.SetProgressor('default', f'Joining {tbl_name} table to geography ...')
    err, fc_path = create_featureclass(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, unique_location_ids)
    
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
        
        # Output Folder for Raw files (Raw)
        param_output_raw_folder = arcpy.Parameter(
            displayName="Output folder for Raw CSVs",
            name="in_gd_outputrawfolder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
           
        # Output CSV Summary files?
        param_output_summary = arcpy.Parameter(
            displayName = "Output summary files",
            name= "in_gd_outputsumm",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        
        # Output Folder for Summary files (Summary)
        param_output_summary_folder = arcpy.Parameter(
            displayName="Output folder for Summary CSVs",
            name="in_gd_outputsummfolder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")


        # Output Workspace
        param_outworkspace = arcpy.Parameter(
            displayName="File Geodatabase Output Workspace for Features and Tables",
            name="in_gd_outgdbworkspace",
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

        
        param_joingeo.enabled=False
        param_geolayer.enabled = False
        param_geojoinfield.enabled = False
        param_keepallgeo.enabled = False
        param_output_summary_folder.enabled = False
        param_geojoinfield.parameterDependencies = [param_geolayer.name]
        
        return [param_url, param_username, param_password, param_outbreak, #4
            param_output_raw_folder, param_output_summary,  param_output_summary_folder,#3
            param_outworkspace, param_joingeo, param_geolayer,#3
            param_geojoinfield, param_keepallgeo, param_outputfcpaths]#3

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
            selected_outbreak_id = outbreaks_cache[parameters[3].value]
        
        parameters[6].enabled = parameters[5].value
        
        parameters[7].enabled = parameters[5].value
        parameters[8].enabled = parameters[5].value

        parameters[9].enabled = parameters[8].value
        parameters[10].enabled = parameters[8].value
        parameters[11].enabled = parameters[8].value
       
        return

    def updateMessages(self, parameters):
        if parameters[5].value ==True & (not parameters[6].value):
            parameters[6].setErrorMessage('Please enter a valid pathway for summary outputs')

        if parameters[5].value ==True & (not parameters[7].value):
            parameters[7].setErrorMessage('Please enter a valid pathway for spatial data outputs')
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
        in_gd_outbreak = selected_outbreak_id
        in_gd_outputrawfolder = parameters[4].valueAsText
        in_gd_outputsumm = parameters[5].value
        in_gd_outputsummfolder = parameters[6].valueAsText
        in_gd_outgdbworkspace = parameters[7].valueAsText
        in_gd_shouldjoin = parameters[8].value    
        in_gd_geolayer = parameters[9].value
        in_gd_geojoinfield = parameters[10].valueAsText
        in_gd_shouldkeepallgeo = parameters[11].value

        #wd = create_working_directory(in_gd_outputrawfolder, in_gd_outputsumm)      
        #full_job_path_raw = wd[0]
        #full_job_path_summ = wd[1]
        #now_ts = wd[2]

        full_job_path_raw = set_working_directory(in_gd_outputrawfolder)
        if in_gd_outputsumm:
            full_job_path_summ = set_working_directory(in_gd_outputsummfolder)

       # wd_raw = create_working_directory(in_gd_outcsvrawfolder, in_gd_outputsumm)
        #full_job_path_raw = wd_raw[0]
        output_paths = []  

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
        seven_days_ago = (right_now - one_week_delta).date()
        eight_days_ago = (right_now - eight_days_delta).date()
        fourteen_days_ago = (right_now - two_week_delta).date()
        fifteen_days_ago = (right_now - fifteen_days_delta).date()
        twenty_eight_days_ago = (right_now - twenty_eight_days_delta).date()
        
        date_flds = ['date', 'dateOfReporting', 'dateOfOnset', 'dateOfInfection', 'dateOfLastContact', 
                    'dateBecomeCase', 'dateOfOutcome', 'dateFollowUpStart' , 'dateFollowUpEnd', 'dateOfBurial']
        dt_flds = ['createdAt', 'updatedAt']
        age_bins = [0, 4, 14, 24, 64, 150]
        age_labels = ['<5', '5-14', '15-24', '25-64', '65+']

        def updateDates(date_flds, dt_flds, df):
            for fld in date_flds:
                try:
                    df[fld] = df[fld].str.split('T').str[0]
                except:
                    pass
            for fld in dt_flds:
                try:
                    df[fld] = df[fld].astype('datetime64[s]')
                except:
                    pass
        
        def getVisualIds(rel_df, right_df, person_type):
            if (len(rel_df)==0) or (len(right_df)==0):
                return rel_df
            if 'visualId' in right_df.columns:
                visual_ids = right_df[['id', 'visualId']].copy()
                visual_ids.drop_duplicates(inplace=True)
                visual_ids.rename(columns = {'id':f'{person_type}_person_id',
                                                'visualId':f'{person_type}_person_visual_id'}, inplace=True)
                return pd.merge(rel_df, visual_ids, how='left', left_on= f'{person_type}_person_id', right_on=f'{person_type}_person_id')
                
        # get reference codes & labels
        # e.g. LNG_REFERENCE_DATA_CATEGORY_OUTCOME_ALIVE = 'Alive'
        arcpy.SetProgressor('default', 'Getting Go.Data reference data')
        ref_data = get_ref_data(in_gd_api_url, token)

        #create locations_df
        arcpy.SetProgressor('default', 'Getting Locations')
        locations = get_locations(in_gd_api_url, token)
        new_locations = convert_loc_json_to_csv(locations, ref_data)
        locations_df = pd.DataFrame(new_locations)  
        locations_df['adminLevel'] = locations_df['geographicalLevelId'].str.split('_').str[-1]
        locations_df.loc[locations_df['adminLevel'].isna(), 'adminLevel'] = -1               
        locations_df['adminLevel'] = locations_df['adminLevel'].astype(int)
        
        #transpose locations data
        i = 0
        while i < 6:
            flds = ['name', 'parentLocationId', 'id', 'Lat', 'Lng']
            currentlocid = f'admin_{i}_LocationId'
            parentlocid = f'admin_{i-1}_LocationId'
            currentname = f'admin_{i}_name'
            lat = f'admin_{i}_Lat'
            lng = f'admin_{i}_Lng'
            if i == 0:
                locations_out = locations_df.loc[locations_df['adminLevel']==i].copy()
                locations_out = locations_out[flds]
                locations_out.rename(columns = {'id': currentlocid,
                                        'name': currentname,
                                        'Lat':lat,
                                        'Lng':lng}, inplace=True)
            else:
                adminlevel = locations_df.loc[locations_df['adminLevel']== i].copy()
                adminlevel = adminlevel[flds]
                adminlevel.rename(columns = {'id': currentlocid,
                                            'parentLocationId': parentlocid,
                                            'name': currentname,
                                            'Lat':lat,
                                            'Lng':lng}, inplace=True)
                locations_out = locations_out.merge(adminlevel, how='left', left_on=parentlocid, right_on=parentlocid)
            i+=1

        locations_out.dropna('columns', how='all', inplace=True)
        locations_out.to_csv(full_job_path_raw.joinpath('Locations.csv'), encoding='utf-8-sig', index=False)

        #splitting the reference data
        def fieldValueSplitter(df, field, splitter, idx= -1):
            if field in df.columns:
                if df[field].isnull().all():
                    pass
                else:
                    df[field] = df[field].str.split(splitter).str[idx]

        # create cases_df
        arcpy.SetProgressor('default', 'Getting Outbreak Cases')
        cases = get_cases(selected_outbreak_id, in_gd_api_url, token)
        new_cases = convert_cases_json_to_csv(cases, ref_data)
        cases_df = pd.DataFrame(new_cases)
        fieldValueSplitter(cases_df, 'classification', 'CLASSIFICATION_')
        fieldValueSplitter(cases_df, 'gender', 'GENDER_')
        fieldValueSplitter(cases_df, 'occupation', 'OCCUPATION_')
        fieldValueSplitter(cases_df, 'pregnancyStatus', 'PREGNANCY_STATUS_')
        fieldValueSplitter(cases_df, 'riskLevel', 'RISK_LEVEL_')
        fieldValueSplitter(cases_df, 'outcomeId', 'OUTCOME_')
        updateDates(date_flds, dt_flds, cases_df)
        if 'age' in cases_df.columns:
            cases_df['ageClass'] = pd.cut(cases_df['age'], bins=age_bins, labels=age_labels)

        # create contacts_df
        arcpy.SetProgressor('default', 'Getting Contact Data')
        contact_data = get_contacts(selected_outbreak_id, in_gd_api_url, token)
        new_contacts = convert_contacts_json_to_csv(contact_data, ref_data)
        contacts_df = pd.DataFrame(new_contacts)
        fieldValueSplitter(contacts_df, 'gender', 'GENDER_')
        fieldValueSplitter(contacts_df, 'occupation', 'OCCUPATION_')
        fieldValueSplitter(contacts_df, 'pregnancyStatus', 'PREGNANCY_STATUS_')
        fieldValueSplitter(contacts_df, 'riskLevel', 'RISK_LEVEL_')
        updateDates(date_flds, dt_flds, contacts_df)
        if 'age' in contacts_df.columns:
            contacts_df['ageClass'] = pd.cut(contacts_df['age'], bins=age_bins, labels=age_labels)
        contacts_df['dateFollowUpStart'] = pd.to_datetime(contacts_df['dateFollowUpStart']).dt.date
        contacts_df['dateFollowUpEnd'] = pd.to_datetime(contacts_df['dateFollowUpEnd']).dt.date
        contacts_df.loc[(contacts_df['dateFollowUpStart']<= yesterday) & (contacts_df['dateFollowUpEnd'] >= right_now.date()), 'underFollowUp'] = True
        contacts_df.loc[contacts_df['underFollowUp'] != True, ['underFollowUp']] = False
        contacts_df.loc[(contacts_df['dateFollowUpStart'] >= seven_days_ago) & (contacts_df['dateFollowUpStart'] <= yesterday), 'followUpPast7Days'] = True
        contacts_df.loc[contacts_df['followUpPast7Days'] != True, ['followUpPast7Days']] = False
        contacts_df.loc[(contacts_df['dateFollowUpStart'] >= fourteen_days_ago) & (contacts_df['dateFollowUpStart'] <= yesterday), 'followUpPast14Days'] = True
        contacts_df.loc[contacts_df['followUpPast14Days'] != True, ['followUpPast14Days']] = False
        
        # create followups_df
        arcpy.SetProgressor('default', 'Getting Followup Data')
        followup_data = get_followups(selected_outbreak_id, in_gd_api_url, token)
        new_followups = convert_followups_json_to_csv(followup_data, ref_data)
        followups_df = pd.DataFrame(new_followups)
        if len(followups_df)>0:
            fieldValueSplitter(followups_df, 'statusId', 'STATUS_TYPE_')
            updateDates(date_flds, dt_flds, followups_df)
            followups = True
        else: 
            followups = False

        # create relates_df
        arcpy.SetProgressor('default', 'Getting Relationship Data')
        relate_data = get_relationships(selected_outbreak_id, in_gd_api_url, token)
        new_relates = convert_relates_json_to_csv(relate_data, ref_data)
        relates_df = pd.DataFrame(new_relates)
        relate_cols = relates_df.columns
        fieldValueSplitter(relates_df, 'source_person_type', 'PERSON_TYPE_')
        fieldValueSplitter(relates_df, 'target_person_type', 'PERSON_TYPE_')
        fieldValueSplitter(relates_df, 'exposureTypeId', 'exposureTypeId')
        fieldValueSplitter(relates_df, 'socialRelationshipTypeId', 'TRANSMISSION_')
        fieldValueSplitter(relates_df, 'exposureDurationId', 'DURATION_')
        fieldValueSplitter(relates_df, 'exposureFrequencyId', 'FREQUENCY_')
        fieldValueSplitter(relates_df, 'certaintyLevelId', 'CERTAINTY_LEVEL_')
        relates_df = getVisualIds(relates_df, contacts_df, 'target')
        relates_df = getVisualIds(relates_df, cases_df, 'source')

        # output relationships data (no joins) 
        relate_model = list(get_FieldNameUpdater('relationship_data')['attributes'].keys())
        relate_model = [c for c in relate_model if c in relates_df.columns] 
        relates_out = relates_df.filter(relate_model)  # reducing the columns
        relates_out = relates_out[relate_model]        # reordering the columns
        updateDates(date_flds, dt_flds, relates_out)
        relates_out.to_csv(full_job_path_raw.joinpath('Relationships.csv'), index=False)
        #relates_df.to_csv(full_job_path_raw.joinpath('Relationships.csv'), index=False)

        #prep locations file for join using the lowest level admin that is found in the cases data
        all_loc_ids =  locations_df[['id', 'adminLevel']].loc[locations_df['adminLevel']!=-1].rename(columns={'id':'locationId'}).set_index('locationId')
        all_cases_loc_ids = cases_df.groupby('locationId', as_index=False).count()[['locationId', 'id']].rename(columns={'id':'cnt'}).set_index('locationId')
        admin_level = all_cases_loc_ids.join(all_loc_ids, how='left').groupby('adminLevel')['cnt'].sum().idxmax()
        location_flds = [f'admin_{i}_name' for i in range(int(admin_level)+1)]
        location_flds.extend([f'admin_{admin_level}_LocationId', f'admin_{admin_level}_Lat', f'admin_{admin_level}_Lng'])
        locations_join = locations_out[location_flds].copy()
        locations_join.rename(columns = {f'admin_{admin_level}_Lat':'Lat',
                                        f'admin_{admin_level}_Lng':'Lng'}, inplace=True)
        
        # Join Locations to Cases and output cases
        cases_df = pd.merge(cases_df, locations_join, how='left', left_on='locationId', right_on=f'admin_{admin_level}_LocationId')
        case_model = list(get_FieldNameUpdater('case_level_data')['attributes'].keys())
        case_model = [c for c in case_model if c in list(cases_df.columns) ] #take the fieldnames from case_model that are also in the dataframe
        cases_df = cases_df.filter(case_model)
        cases_df = cases_df[case_model]
        cases_df.to_csv(full_job_path_raw.joinpath('Cases.csv'), index=False) 

        # Join Locations to Followups and output followups
        if followups:
            followups_df = pd.merge(followups_df, locations_join, how='left', left_on='locationId', right_on=f'admin_{admin_level}_LocationId')
            followup_model = list(get_FieldNameUpdater('followup_data')['attributes'].keys())
            followup_model = [c for c in followup_model if c in list(followups_df.columns)]
            followups_df = followups_df.filter(followup_model)
            followups_df = followups_df[followup_model]
            followups_df.to_csv(full_job_path_raw.joinpath('Followups.csv'), index=False)
        else:
            arcpy.AddMessage('There are no data for followups, skipping Followups.csv output')
        
        # # Join Locations to Contacts and output contacts
        contacts_df = pd.merge(contacts_df, locations_join, how='left', left_on='locationId', right_on=f'admin_{admin_level}_LocationId')
        contact_model = list(get_FieldNameUpdater('contact_data')['attributes'].keys())
        contact_model = [c for c in contact_model if c in list(contacts_df.columns)]
        contacts_df = contacts_df.filter(contact_model)
        contacts_df=contacts_df[contact_model]
        contacts_df.to_csv(full_job_path_raw.joinpath('Contacts.csv'), index=False)

        start_date = min(list([datetime.strptime(c['dateOfReporting'], dte_format).date() for c in new_cases]))
        
        if in_gd_outputsumm:
            # Cases by Reporting Area
            features = []
            for case in new_cases:
                location_id = case['locationId']   
                reporting_date = datetime.strptime(case['dateOfReporting'], dte_format).date()      
                feature = get_feature(location_id, features, 'cases_by_reporting_area') # get/create the dictionary for the location_id
                # yesterday
                if reporting_date == yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'DAILY_NEW_CONFIRMED')
                # cumulative
                if  start_date <= reporting_date <= yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'CUM_CONFIRMED')                    
                #last week
                if seven_days_ago <= reporting_date <= yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'CONFIRMED_LAST_SEVEN')
                #last two weeks
                if fourteen_days_ago <= reporting_date <= yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'CONFIRMED_LAST_FOURTEEN')
            # convert features back to csv_rows
            headers, cases_by_rep_csv_rows = convert_features_to_csv(features)

            # create cases CSV
            arcpy.SetProgressor('default', 'Creating Cases CSV file ...')
            path_to_csv_file = create_csv_file(cases_by_rep_csv_rows, 'Cases_by_Reporting_Area.csv', headers, full_job_path_summ)

            # create cases FC table
            arcpy.SetProgressor('default', 'Creating Cases feature class table ...')
            output_filename = 'Cases_By_Reporting_Area'
            err = create_fc_table(str(path_to_csv_file), in_gd_outgdbworkspace, output_filename)
            if err is not None:
                arcpy.AddError(err)

            unique_location_ids = []
            for c in new_cases:
                for k in c.keys():
                    if k == 'locationId':
                        if not c[k] in unique_location_ids:
                            unique_location_ids.append(c[k])

            if in_gd_shouldjoin:
                # create cases FC table
                err, fc_path = join_to_geo(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Cases by Reporting Area', unique_location_ids)
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
                if 'dateOfOutcome' in case:
                    
                    death_date = datetime.strptime(case['dateOfOutcome'], dte_format).date()
                    
                    feature = get_feature(location_id, deaths_features, 'deaths_by_reporting_area') 
            
                    if start_date  <= death_date <= yesterday:
                        increment_count(feature, 'CUM_DEATHS')                    
            
                    if seven_days_ago <= death_date <= yesterday:
                        increment_count(feature, 'DEATHS_LAST_SEVEN')

                    if fourteen_days_ago <= death_date <=  yesterday:
                        increment_count(feature, 'DEATHS_LAST_FOURTEEN')


            if len(deaths_features) > 0:
                # convert back to csv
                headers, deaths_by_rep_csv_rows = convert_features_to_csv(deaths_features)
                # create deaths CSV
                arcpy.SetProgressor('default', 'Creating Deaths by Reporting Area CSV file ...')
                path_to_csv_file = create_csv_file(deaths_by_rep_csv_rows, 'Deaths_by_Reporting_Area.csv', headers, full_job_path_summ)
            else:
                arcpy.AddMessage('No death data available.  Skipping Deaths_by_Reporting_Area.csv')

            # create deaths FC table
            arcpy.SetProgressor('default', 'Creating Deaths by Reporting Area feature class table ...')
            output_filename = 'Deaths_By_Reporting_Area'
            err = create_fc_table(str(path_to_csv_file), in_gd_outgdbworkspace, output_filename)
            if err is not None:
                arcpy.AddError(err)

            # join to geography
            if in_gd_shouldjoin:
                err, fc_path = join_to_geo(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Deaths by Reporting Area', unique_location_ids)
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

            for case in new_cases:
                location_id = case['locationId']
        
                reporting_date = datetime.strptime(case['dateOfReporting'], dte_format).date()
           
                feature = get_feature(location_id, pctchg_features, 'pctchg_by_reporting_area')                   
        
                if seven_days_ago <= reporting_date <=  yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'AVG_CONFIRMED_LAST_SEVEN')
                    
                if fourteen_days_ago <= reporting_date <=  yesterday:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'AVG_CONFIRMED_LAST_FOURTEEN')
                
                if fourteen_days_ago <= reporting_date <=  eight_days_ago:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'AVG_CONFIRMED_EIGHT_TO_FOURTEEN')
                            
                if twenty_eight_days_ago <= reporting_date <=  fifteen_days_ago:
                    if case['classification_code'] == 'LNG_REFERENCE_DATA_CATEGORY_CASE_CLASSIFICATION_CONFIRMED':
                        increment_count(feature, 'AVG_CONFIRMED_FIFTEEN_TO_TWENTY_EIGHT')

            for feature in pctchg_features:
                val = feature['attributes']['AVG_CONFIRMED_LAST_SEVEN']
                if val is not None:
                    feature['attributes']['AVG_CONFIRMED_LAST_SEVEN'] = round(val / 7, 2)
            
                val = feature['attributes']['AVG_CONFIRMED_LAST_FOURTEEN']
                if val is not None:
                    feature['attributes']['AVG_CONFIRMED_LAST_FOURTEEN'] = round(val / 14, 2)
        
                val = feature['attributes']['AVG_CONFIRMED_EIGHT_TO_FOURTEEN']
                if val is not None:
                    feature['attributes']['AVG_CONFIRMED_EIGHT_TO_FOURTEEN'] = round(val / 7, 2)
            
                val = feature['attributes']['AVG_CONFIRMED_FIFTEEN_TO_TWENTY_EIGHT']
                if val is not None:
                    feature['attributes']['AVG_CONFIRMED_FIFTEEN_TO_TWENTY_EIGHT'] = round(val / 14, 2)
            
                val2 = feature['attributes']['AVG_CONFIRMED_LAST_SEVEN']  
                val1 = feature['attributes']['AVG_CONFIRMED_EIGHT_TO_FOURTEEN']
                if val2 is not None and val1 is not None and val2 > 0 and val1 > 0:
                    pct_chg = ((val2 - val1) / abs(val1)) * 100  
                    feature['attributes']['PERCENT_CHANGE_RECENT_SEVEN'] = pct_chg
            
                val2 = feature['attributes']['AVG_CONFIRMED_LAST_FOURTEEN']
                val1 = feature['attributes']['AVG_CONFIRMED_FIFTEEN_TO_TWENTY_EIGHT']
                if val2 is not None and val1 is not None and val2 > 0 and val1 > 0:
                    pct_chg = ((val2 - val1) / abs(val1)) * 100
                    feature['attributes']['PERCENT_CHANGE_RECENT_FOURTEEN'] = pct_chg
        
            # convert back to csv
            headers, pct_by_rep_csv_rows = convert_features_to_csv(pctchg_features)

            # create pct change CSV
            arcpy.SetProgressor('default', 'Creating Percent Change in New Cases by Reporting Area CSV file ...')
            path_to_csv_file = create_csv_file(pct_by_rep_csv_rows, 'Percent_Change_in_New_Cases_by_Reporting_Area.csv', headers, full_job_path_summ)

            # create pct change FC table
            arcpy.SetProgressor('default', 'Creating Percent Change in New Cases by Reporting Area feature class table ...')
            output_filename = 'Percent_Change_in_New_Cases_by_Reporting_Area'
            err = create_fc_table(str(path_to_csv_file), in_gd_outgdbworkspace, output_filename)
            if err is not None:
                arcpy.AddError(err)

            # join to geography
            if in_gd_shouldjoin:
                err, fc_path = join_to_geo(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Percent Change in New Cases by Reporting Area', unique_location_ids)
                if err is not None:
                    arcpy.AddError(err)

                output_paths.append(fc_path)

            # Active Contacts Summary Table
            arcpy.SetProgressor('default', 'Creating Contacts by Reporting Area CSV file ...')
            output_filename = 'Contacts_by_Reporting_Area'
            path_to_csv_file = full_job_path_summ.joinpath('Contacts_by_Reporting_Area.csv').resolve()
            contacts_summary = contacts_df[['locationId' ,f'admin_{admin_level}_name','underFollowUp', 'followUpPast7Days', 'followUpPast14Days']].copy()
            contacts_summary.replace({True:1,False:0}, inplace=True)
            contacts_summary.rename(columns={'underFollowUp':'UNDER_FOLLOWUP','followUpPast7Days':'PAST_7_DAYS', 'followUpPast14Days':'PAST_14_DAYS'}, inplace=True)
            contacts_summary = contacts_summary.groupby(['locationId', f'admin_{admin_level}_name'])[['UNDER_FOLLOWUP', 'PAST_7_DAYS', 'PAST_14_DAYS']].sum()
            contacts_summary = contacts_summary.loc[contacts_summary.sum(axis=1)>0]
            contacts_summary.reset_index(inplace=True)
            unique_location_ids = list(set(contacts_summary['locationId'].unique()))
            contacts_summary.to_csv(path_to_csv_file, index=False)
            
            arcpy.SetProgressor('default', 'Creating Contacts_by_Reporting_Area feature class table ...')
            err = create_fc_table(str(path_to_csv_file), in_gd_outgdbworkspace, output_filename)
            if err is not None:
                arcpy.AddError(err)

            # join to geography
            if in_gd_shouldjoin:
                err, fc_path = join_to_geo(path_to_csv_file, in_gd_outgdbworkspace, output_filename, in_gd_geolayer, in_gd_geojoinfield, in_gd_shouldkeepallgeo, 'Contacts_by_Reporting_Area', unique_location_ids)
                if err is not None:
                    arcpy.AddError(err)

                output_paths.append(fc_path)

            if len(output_paths) > 0:
                # set the output parameter
                arcpy.SetParameter(12, ';'.join(output_paths))
        
        return 
