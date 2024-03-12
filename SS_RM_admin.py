#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import *
import pandas as pd
from logger import ghetto_logger
#endregion

class SmartsheetRmAdmin():
    '''admin for DCT's Resource Management tool that is part of SS'''
    def __init__(self, config):
        self.config = config
        self.apply_config(config)
        grid.token=self.smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.start_time = time.time()
        self.log=ghetto_logger("SS_RM_admin.py")
    #region helpers
    def apply_config(self, config):
        '''turns all config items into self.key = value'''
        for key, value in config.items():
            setattr(self, key, value)

    def validate_and_contains_first_row(self, dataframe):
        '''Checks if all columns have the words from the first row (minus the last, which is row ids)
        this is important because that match represents that the data DCT pastes in matches what this script is expecting to see'''
        for i in range(len(dataframe.columns)-1):
            # Check if the value of each cell in the first row is contained within the corresponding column name
            if str(dataframe.iloc[0, i]) not in dataframe.columns[i]:
                return False  # If any cell is not contained, return False
        return True  # If all cells are contained, return True
    #endregion
    #region Time & Expense
    def grab_rm_userids(self):
        '''grabs each user's id, this will help with allocating hours to users correctly'''
        headers = {
            'Content-Type': 'application/json',
            'auth': self.rm_token
        }

        response = requests.get('https://api.rm.smartsheet.com/api/v1/users', headers=headers)

        response_dict = response.json()

        self.rm_user_list=[]
        self.sageid_to_email={}
        for user in response_dict['data']:
            self.rm_user_list.append({'email': user['email'].lower(), 'rm_usr_id':  user['id'], 'name': user['display_name'], 'sage id': user['employee_number']})
            self.sageid_to_email[user['employee_number']] = user['email'].lower()
    def grab_rm_projids(self):
        '''grabs each project's id from RM in SS'''
        headers = {
            'Content-Type': 'application/json',
            'auth': self.rm_token
        }

        response = requests.get('https://api.rm.smartsheet.com/api/v1/projects?sort_field=created&sort_order=ascending', headers=headers)

        response_dict = response.json()

        self.rm_proj_list=[]
        for proj in response_dict['data']:
            if proj['project_code'] != "":
                self.rm_proj_list.append({'project name':proj['name'],  'job number':proj['project_code'], 'rm_proj_id':proj['id']})
        #region remedy no sage id
    def grab_sage_id_dict(self):
        '''grab sage id // email dict from ss'''
        sheet = grid(self.hris_data_sheetid)
        sheet.fetch_content()       

        self.sage_id_dict = {}
        for index, row in sheet.df.iterrows():
            try: 
                email = row['emailAsText'].lower()
                self.sage_id_dict[email] = row['sage_id']
            except AttributeError:
                email = ''
    def post_user_emplnum(self):
        '''updates employee to have employee number'''
        for user in self.needs_emplnum_update:
            headers = {
                'Content-Type': 'application/json',
                'auth': self.rm_token
            }

            data = {
                'employee_number': self.sage_id_dict[user['email'].lower()]
            }

            response = requests.put(f"https://api.rm.smartsheet.com/api/v1/users/{user['rm_usr_id']}", headers=headers, data=json.dumps(data))

            if response.status_code == 200:
                self.log.log(f"Added EmpployeeNumber to {user['name']}'s user data")

            response_dict = response.json()
    def audit_users_emplnum(self):
        '''if new employee does not have employee number: spot, grab sage_id, post'''

        self.needs_emplnum_update = []
        for user in self.rm_user_list:
            if user['sage id'] == '':
                self.needs_emplnum_update.append(user)
        
        if len(self.needs_emplnum_update) > 0: 
            self.grab_sage_id_dict()
            self.post_user_emplnum()
            time.sleep(5)
            self.grab_rm_userids()
        #endregion 
    def process_hh2_data(self):
        '''processes HH2 data posted https://app.smartsheet.com/sheets/GffHvGGxVJwQ9P8w8gwgfqrmJjcq39JXvMQmH7q1?view=grid, to compare against RM'''
        sheet = grid(self.hh2_data_sheetid)
        sheet.fetch_content()     
        df = sheet.df

        if not self.validate_and_contains_first_row(sheet.df):
            error = "the first row of the HH2 Smartsheet does not match the column ids, please check the data that was posted, and clear accordingly. If a fundimental change to the data was done, contact IT"
            self.log.log(error)
        else:
            df.drop(index=df.index[0], inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Filter the DataFrame to keep only rows where ApprovalType is 'Submitted'
            filtered_df = df[df['ApprovalType'] == 'Submitted'].copy()

            # Convert 'Date' to datetime
            filtered_df.loc[:, 'Date'] = pd.to_datetime(filtered_df['Date'])

            # Ensure 'Description' is a string
            filtered_df.loc[:, 'Description'] = filtered_df['Description'].astype(str)

            # Convert 'Units' to numeric, coercing non-numeric to NaN
            filtered_df.loc[:, 'Units'] = pd.to_numeric(filtered_df['Units'], errors='coerce')

            # Group by the necessary columns and aggregate
            grouped = filtered_df.groupby(['Job', 'Date', 'EmployeeNumber', 'CostCodeName']).agg({
                'Units': 'sum',  # Sum of 'Units' for total
                'Description': ' '.join  # Concatenate 'Description'
            }).reset_index()

            # Initialize a list to store the flat records
            flat_records = []

            for index, row in grouped.iterrows():
                employee_email = self.sageid_to_email[row['EmployeeNumber']]
                job = row['Job']
                date = row['Date'].date().isoformat()  # Convert datetime to ISO format date string
                hours = row['Units']
                description = row['Description']
                cost_code_name = row['CostCodeName']

                record = {
                    "user": employee_email,
                    "assignable_id": job,
                    "date": date,
                    "hours": hours,
                    "task": cost_code_name,
                    "notes": description
                }

                flat_records.append(record)

            # Convert the list of records to a DataFrame
            self.flat_df = pd.DataFrame(flat_records)
    #endregion
    #region Project Syncing
    def grab_proj_sheetids(self):
        '''grabs the sheet ids of projects from the workspace id'''
        self.sheet_ids = {}
        for sheet in self.smart.Workspaces.get_workspace(self.proj_workspace_id).to_dict()['sheets']:
            self.sheet_ids[sheet['name']] = sheet['id']
    def establish_sheet_connection(self):
        '''checks sheet names against proj names in RM (also looking to see if the sheet name minus last character (which could be *) matches something in RM. 
        if there is a match, its status is "connected", if not its status is "disconnected"'''
        self.ss_proj_list = []
        for sheet_name in self.sheet_ids:
            connected = False  # Flag to track connection status
            for rm_proj in self.rm_proj_list:
                rm_id=''
                if rm_proj['project name'] == sheet_name or rm_proj['project name'] == sheet_name.rstrip('*'):
                    connected = True
                    rm_id = rm_proj['rm_proj_id']
                    break  # Exit loop early if a match is found
            status = 'connected' if connected else 'disconnected'
            self.ss_proj_list.append({'name': sheet_name, 'ss_sheet_id': self.sheet_ids[sheet_name], 'rm_id':rm_id, 'status': status})
    def update_sheet_name(self, sheet_info):
        '''adds star to end of all sheet names that need it'''
        if (sheet_info['status'] == "disconnected" and sheet_info['name'].endswith('*')) or (sheet_info['status'] == "connected" and not sheet_info['name'].endswith('*')):
            return
        elif sheet_info['status'] == "disconnected":
            new_name=sheet_info['name'] + "*"
        else:
            new_name= sheet_info['name'][:len(sheet_info['name'])-1]
        try:
            updated_sheet = self.smart.Sheets.update_sheet(
            # sheet id
            sheet_info['ss_sheet_id'], 
            # new name
            smartsheet.models.Sheet({
                'name': new_name}))
        except Exception as e:
            self.log.log(f"Error updating sheet name: {e}")
    def grab_connected_sheet_data(self, sheet_i, sheet_info):
        '''if the sheet is connected, grab the nessisary data'''
        if sheet_info['status'] == "connected":
            sheet = grid(sheet_info['ss_sheet_id'])
            sheet.fetch_summary_content()
            self.parent_data= sheet.df.to_dict('records')
            meta_data = {sum_field['title']: sum_field['displayValue'] for sum_field in self.parent_data if sum_field['title'] in ['Project Enumerator [MANUAL ENTRY]', 'DCT Status', 'Build Region', 'Build Job Number', 'Build Architect']}
            self.ss_proj_list[sheet_i]['meta_data'] = meta_data
    def get_rmproj_metadata(self, proj):
        '''checks connected projects for sync of meta data, and compares. If out of sync, sounds to api call'''
        url = f"https://api.rm.smartsheet.com/api/v1/projects/{proj['rm_id']}"
        headers = {
            'Content-Type': 'application/json',
            'auth': self.rm_token
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            rm_meta_data= {'proj_status':response.json()['tags']['data'][0]['value'], 'job_num':response.json()['project_code'], "region":response.json()['client']}

            if not rm_meta_data['proj_status'] == proj['meta_data']['DCT Status'] and rm_meta_data['job_num'] == proj['meta_data']['Build Job Number'] and rm_meta_data['region'] == proj['meta_data']['Build Region']:
                self.update_rm_proj(self, rm_meta_data, proj)

        else:
            self.log.log(f"{proj['name']} could not be found on RM")

    def update_rm_proj(self, rm_meta_data, proj):
        '''EDIT updates employee to have employee number'''
        headers = {
            'Content-Type': 'application/json',
            'auth': self.rm_token
        }
        data = {
            'id':proj['rm_id'],
            'project_code':rm_meta_data['job_num'],
            'region':rm_meta_data['region'],
        }
        response = requests.put(f"https://api.rm.smartsheet.com/api/v1/projects/{proj['rm_id']}", headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            self.log.log(f"Updated {proj['name']}'s meta data")
        response_dict = response.json()
    #endregion



    def run_hours_update(self):
        '''runs main script as intended'''
        self.grab_rm_userids()
        self.audit_users_emplnum()
        self.grab_rm_projids()
        self.process_hh2_data()
        grid(self.hh2_data_sheetid).handle_update_stamps()

    def run_proj_metadata_update(self):
        '''katherine has mapped particular columns of her project template to meta data fields in RM, this script keeps it up to date'''
        self.grab_proj_sheetids()
        self.establish_sheet_connection()
        for proj_i, proj in enumerate(self.ss_proj_list):
            print(proj['name'])
            self.update_sheet_name(proj)
            if proj['status'] == 'connected':
                self.grab_connected_sheet_data(proj_i, proj)
                self.get_rmproj_metadata(proj)

if __name__ == "__main__":
    # https://app.smartsheet.com/sheets/GffHvGGxVJwQ9P8w8gwgfqrmJjcq39JXvMQmH7q1?view=grid is hh2 data sheet
    # https://app.smartsheet.com/browse/workspaces/GXmwRM4wcCmjMVGVjhJ2cWCFR9QWMQCr5w8WGrx1 is proj workspace
    config = {
        'smartsheet_token':smartsheet_token,
        'rm_token':rm_token,
        'hh2_data_sheetid': 1780078719487876,
        'hris_data_sheetid': 5956860349048708,
        'proj_workspace_id': 4883274435716996,
        'proj_list_sheetid': 3858046490306436
        
    }
    sra = SmartsheetRmAdmin(config)
    sra.run_hours_update()
    sra.run_proj_metadata_update()