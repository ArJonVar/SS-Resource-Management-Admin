#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from datetime import datetime
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
        self.rm_header = {
            'Content-Type': 'application/json',
            'auth': self.rm_token
        }
        self.error_w_hh2sheet = []
        self.base_url='https://api.rm.smartsheet.com'
    #region helpers
    def apply_config(self, config):
        '''turns all config items into self.key = value'''
        for key, value in config.items():
            setattr(self, key, value)
    def validate_and_contains_first_row(self, dataframe):
        '''Checks if all columns have the words from the first row (minus the last, which is row ids)
        this is important because that match represents that the data DCT pastes in matches what this script is expecting to see'''
        correct_columns = []
        incorrect_columns = []
        for i in range(len(dataframe.columns)-1):
            # Check if the value of each cell in the first row is contained within the corresponding column name
            if str(dataframe.iloc[0, i]) in dataframe.columns[i]:
                correct_columns.append(str(dataframe.iloc[0, i]))
            # check each column to make sure it is in the correct columns list, otherwise it is missing and therefore wrong
        for column in dataframe.columns.tolist():
            if column !='id' and column not in correct_columns:
                incorrect_columns.append(column)
        return incorrect_columns  # If all cells are contained, return True
    def paginated_rm_getrequest(self, endpoint, params=None):
        """
        Fetches data from an API endpoint. Handles both single item and paginated responses.

        :param endpoint: The specific endpoint to fetch data from.
        :param headers: Dictionary containing request headers.
        :param params: Dictionary containing any query parameters for the GET request.
        :return: A single item or a list of items aggregated from all pages.
        """
        url = f"{self.base_url}{endpoint}"
        items = []
        while url:
            response = requests.get(url, headers=self.rm_header, params=params)
            if response.status_code == 200:
                response_json = response.json()
                # Check if response is paginated
                if 'data' in response_json:
                    items.extend(response_json.get('data', []))
                    next_page = response_json.get('paging', {}).get('next')
                    url = f"{self.base_url}{next_page}" if next_page and not next_page.startswith('http') else next_page
                else:
                    return response_json  # Return a single item
            else:
                self.log.log(f"Failed to fetch data: {response.status_code} - {response.reason}")
                break  # Exit loop on failure
        return items if items else []
    def convert_date_format(self, original_date):
        '''converst YEAR-0DAY-0MONTH to day/month/year'''
        year, month, day = original_date.split('-')
        month = str(int(month))  # Remove leading zero
        day = str(int(day))  # Remove leading zero
        return f"{month}/{day}/{year}"
    def generate_now_string(self):
        '''generates now string for psoting'''
        now = datetime.now()
        dt_string = now.strftime("%m/%d %H:%M")
        return dt_string
    def return_email_list(self, sheet_id, df):
        '''OUTDATED grabs all sheet data, and returns a list of emails that is in the same order as the row (which can be used to filter out emails not in RM)'''
        response = requests.get(f'https://api.smartsheet.com/2.0/sheets/{sheet_id}?level=2&include=objectValue', headers={'Authorization': f"Bearer {sra.smartsheet_token}"})
        if response.status_code == 200:
            self.data = response.json()
            self.i = self.find_email_index(self.data, df)
            email_list = []
            for row in self.data:
                try:
                    email_list.append(row['cells'][self.i]['objectValue']['name'])
                # blank rows won't have objectValue in them
                except KeyError:
                    pass
        else:
            self.log.log('error with grabbing emails from sheet...')
    def find_email_index(self, data, df):
        '''OUTDATED used to find the column index that has PRIMARY DCT so I can grab emails via requests library and column index (and use this to filter out emails that are not already in RM)'''
        for row in data['rows']:
            for i, cell in enumerate(row['cells']):
                if isinstance(cell.get('objectValue'), dict):
                    if cell.get('objectValue').get('name') == df['PRIMARY DCT'].tolist()[0]:
                        return i
    def grab_rm_userids(self):
        '''grabs each user's id, this will help with allocating hours to users correctly'''
        response_dict = self.paginated_rm_getrequest(endpoint='/api/v1/users')

        self.rm_user_list=[]
        self.sageid_to_email={}
        self.userid_to_email={}
        self.email_to_userid={}
        self.email_to_sageid = {}
        for user in response_dict:
            self.rm_user_list.append({'email': user['email'].lower(), 'rm_usr_id':  user['id'], 'name': user['display_name'], 'sage id': user['employee_number']})
            self.sageid_to_email[user['employee_number']] = user['email'].lower()
            self.email_to_sageid[user['email'].lower()] = user['employee_number']
            self.userid_to_email[user['id']] = user['email'].lower()
            self.email_to_userid[user['email'].lower()] = user['id']
    def grab_rm_projids(self):
        '''grabs each project's id from RM in SS, also makes dict that can translate rm_id to job number for time & expense'''
        response_dict = self.paginated_rm_getrequest(endpoint='/api/v1/projects?sort_field=created&sort_order=ascending&with_archived=true')

        self.rm_proj_list=[]
        self.rm_id_to_jobnum = {}
        self.jobnum_to_rm_id = {}
        # for me lol
        self.jobnum_to_name={}
        for proj in response_dict:
            if proj['name'] != "":
                original_jobnumn = proj['project_code']
                if isinstance(original_jobnumn, str):
                    if original_jobnumn.find('.') != -1:
                        proj['project_code'] = original_jobnumn[:original_jobnumn.find('.')]
                    self.jobnum_to_name[proj['project_code']]=proj['name']
                    self.jobnum_to_rm_id[proj['project_code']] = proj['id']
                else:
                    pass

                self.rm_proj_list.append({'project name':proj['name'],  'job number':proj['project_code'], 'rm_proj_id':proj['id']})
                self.rm_id_to_jobnum[proj['id']] = proj['project_code']
    #endregion
    #region Time & Expense
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
            data = {
                'employee_number': self.sage_id_dict[user['email'].lower()]
            }

            response = requests.put(f"https://api.rm.smartsheet.com/api/v1/users/{user['rm_usr_id']}", headers=self.rm_header, data=json.dumps(data))

            if response.status_code == 200:
                self.log.log(f"Added EmpployeeNumber to {user['name']}'s user data")

            response_dict = response.json()
    def audit_users_emplnum(self):
        '''if new employee does not have employee number: spot, grab sage_id, post'''

        self.needs_emplnum_update = []
        for user in self.rm_user_list:
            if user['sage id'] == '' or user['sage id'] == None:
                self.needs_emplnum_update.append(user)
        
        if len(self.needs_emplnum_update) > 0: 
            self.grab_sage_id_dict()
            self.post_user_emplnum()
            time.sleep(5)
            self.grab_rm_userids()
        #endregion 
    def fetch_and_prepare_hh2_data(self):
        '''grabs the hh2 data from ss, then cleans the df and creates a list of dict records'''
        sheet = grid(self.hh2_data_sheetid)
        sheet.fetch_content()
        df = sheet.df
        self.scriptkey_to_script_message = pd.Series(df['Script Message'].values,index=df['Script Key']).to_dict()

        df = df.drop(['Script Message', 'Script Key',  'Repeater Key', 'Message Repeater', 'Formatter'], axis=1)

        invalid_column_list = self.validate_and_contains_first_row(df)

        if invalid_column_list == []:
            df = self.clean_df_for_processing(df)
            self.flat_hh2_records = self.aggregate_hh2_data(df)
        else: 
            self.error_w_hh2sheet.append(f"First row validation failed (so script did not run properly). Please check {invalid_column_list} columns. ({self.generate_now_string()})")
            self.log.log(f'HH2 Sheet error: please check the following column(s) {invalid_column_list} at https://app.smartsheet.com/sheets/GffHvGGxVJwQ9P8w8gwgfqrmJjcq39JXvMQmH7q1?view=grid&filterId=3306346053062532')
            return None  # Return to avoid further processing
    def clean_df_for_processing(self, df):
        '''cleans incoming hh2 data from smartsheet'''
        df.drop(index=df.index[0], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Description'] = df['Description'].astype(str)
        df['Units'] = pd.to_numeric(df['Units'], errors='coerce')
        return df
    def aggregate_hh2_data(self, df):
        '''filteres by approval type, then turns the df into a dict w recrds, 
        making sure to add all units incase there are two entries for the same day/job number'''
        # Chat GPT helped me with this to make it run faster:
        grouped = df[df['ApprovalType'] == 'Sealed'].groupby(
        ['Job', 'Date', 'EmployeeNumber']
        ).agg({
            'Units': 'sum',
            'Description': lambda x: ' '.join(x),
            'CostCodeName': lambda x: ' | '.join(x)
        }).reset_index()
        
        # Assuming EmployeeNumber to email mapping is preprocessed if possible
        # For direct transformation without .iterrows()
        grouped['user'] = grouped['EmployeeNumber'].map(self.sageid_to_email).fillna('default_email@example.com')
        grouped['date'] = pd.to_datetime(grouped['Date']).dt.date.astype(str)  # Ensuring date format
        grouped['rm_user_id'] = grouped['user'].apply(
            lambda x: str(int(self.email_to_userid.get(x.lower()))) if self.email_to_userid.get(x.lower()) is not None else None
        )
        grouped['rm_proj_id'] = grouped['Job'].apply(lambda x: self.jobnum_to_rm_id.get(x, ''))

        # Calculate min and max dates
        self.min_date = grouped['date'].min()
        self.max_date = grouped['date'].max()
        
        # Directly construct the records without explicit row iteration
        records = grouped.to_dict('records')
        self.records = records
        
        # Transform records into desired format
        flat_hh2_records = [{
            "user_email": record['user'],
            "rm_userid": record['rm_user_id'],
            "job_num": record['Job'],
            'rm_proj_id': record['rm_proj_id'],
            "date": record['date'],
            "hours": record['Units'],
            "task": record['CostCodeName'],
            "notes": record.get('Description'),
            # "ss_row_id": record['id'],
            "key" : f"{self.email_to_sageid.get(record['user'])}{self.convert_date_format(record['date'])}{record['Job']}Sealed",
            "messages": []
        } for record in records]
    
        return flat_hh2_records
    def grab_rm_timedata(self):
        '''grabs existing data from rm, translates rm job id to job number, rm user id to user email, 
        and then builds out a reference dictionary of time entries (hrs) for verifying if update is needed, adding hours for same job/time as needed
        and building reference of entry ids w list of ids per entry'''
        self.current_rm_timedata = []
        self.rm_quickreference_hrs = {}
        self.rm_quickreference_id = {}
        for user in self.rm_user_list:
            self.current_rm_timedata.extend(sra.paginated_rm_getrequest(f"/api/v1/users/{user['rm_usr_id']}/time_entries"))
        for timeentry in self.current_rm_timedata:
            timeentry['job_num'] = self.rm_id_to_jobnum[timeentry['assignable_id']]
            timeentry['usr_email'] = self.userid_to_email[timeentry['user_id']]
            key = f"{timeentry['usr_email'].lower()}{timeentry['date']}{timeentry['job_num']}"
            if key not in self.rm_quickreference_hrs:
                self.rm_quickreference_hrs[key] = timeentry['hours']
                self.rm_quickreference_id[key] = [timeentry['id']]  # Initialize with a list containing the id
            else:
                old_number = self.rm_quickreference_hrs[key]
                self.rm_quickreference_hrs[key] = old_number + timeentry['hours']
                self.rm_quickreference_id[key].append(timeentry['id'])  # Directly append the new id to the list
    def process_timedata_discrepencies(self):
        '''compare hh2 data (on ss) w/ rm data. The end result is a list of time entries and their needed actions'''
        up_to_date, to_update, to_add, self.to_add_projntime=0,0,0,0
        self.undeployed_job_nums = []
        for timeentry in self.flat_hh2_records:
            key = f"{timeentry['user_email'].lower()}{timeentry['date']}{timeentry['job_num']}"
            try:
                # print(key, self.rm_quickreference[key], timeentry['hours'])
                if self.rm_quickreference_hrs[key] != timeentry['hours']:
                    timeentry['action'] = 'update'
                    timeentry['rm_entry_id'] = self.rm_quickreference_id[key]
                    to_update += 1
                else:
                    timeentry['action'] = 'current'
                    up_to_date += 1
            except KeyError:
                timeentry['action'] = 'add'
                if timeentry['rm_proj_id'] == '':
                    timeentry['messages'].extend([f"FAILED TO PROCESS: Job Number {timeentry['job_num']} is not in the system, so cannot post time to a time entry ({self.generate_now_string()})"])
                    self.to_add_projntime +=1
                    if timeentry['job_num'] not in self.undeployed_job_nums:
                        self.undeployed_job_nums.append(timeentry['job_num'])
                else:
                    to_add += 1
                continue
        self.log.log(f"""Of the SS/HH2 Time Entries between {self.min_date} and {self.max_date}: 
    {up_to_date} entries current,
    {to_update} entries needing update
    {to_add} entries need to be added
    {self.to_add_projntime} entries that first need project added, then time added""")
        #region post data to rm
    def post_rm_time_changes(self):
        'processes and posts time changes. It tracks job numbers not in RM, error messages, and generally posts action results and a summary of everything it did'
        self.api_error_messages = []
        self.api_error_messages_instance, successful_update, successful_add = 0, 0, 0
        success = False
        # actions
        for i, entry in enumerate(self.flat_hh2_records):
            action = entry.get('action')
            if action== "add":
                success= self.add_new_timedata(entry)
            elif action== "update":
                success= self.delete_old_timedata(entry) and self.add_new_timedata(entry)
            elif action == "current":
                entry['messages'].append(f"Job was current with {entry['hours']}, no action excuted ({self.generate_now_string()})")

        # loging actions
            if success:
                entry['messages'].append(f"Successful post of {entry['hours']} ({self.generate_now_string()})")
                if action == 'add':
                    successful_add += 1
                elif action == 'update':
                    successful_update += 1

        # summary of action
        if self.to_add_projntime > 0:
            self.log.log(f"There was {self.to_add_projntime} instances where a time entry post was attempted on a job we didn't have in the Resouce manager, these were for job(s): {self.undeployed_job_nums}")
        if self.api_error_messages != []:
            self.log.log(f"There was {self.api_error_messages_instance} instances where a time entry post failed due to api error, those errors were: {self.api_error_messages}")
        if successful_update > 0 or successful_add > 0:
            self.log.log(f"~~Time Entry adjustedments are complete, there was {successful_add} successful time entries added and {successful_update} successful time entries updated~~")
    def delete_old_timedata(self, timeentry):
        '''updates will add new and old hours, so we need to first delete old data before posting new'''
        result_list = []
        for id in timeentry['rm_entry_id']:
            result_list.append(requests.delete(f"{self.base_url}/api/v1/users/{timeentry['rm_userid']}/time_entries/{id}", headers=self.rm_header).status_code)
        if not all(code == 200 for code in result_list):
            timeentry['messages'].extend([f"FAILED PREPOST DELETION: incorrect hours associated with this time/user/job number failed to delete ({self.generate_now_string()})"])
        return all(code == 200 for code in result_list)
    def add_new_timedata(self, timeentry):
        '''this posts the correct time data
        noting if an error was raised, or if there was no project id in RM to correspond with the job number'''
        data = {
            'user_id':timeentry['rm_userid'],
            'assignable_id':timeentry['rm_proj_id'],
            'date': timeentry['date'],
            'hours': timeentry['hours'],
            'task': timeentry['task'],
            'notes':timeentry['notes'][0:254]
        }
        if timeentry['rm_proj_id']:
            result = requests.post(
                url = f"{self.base_url}/api/v1/users/{timeentry['rm_userid']}/time_entries",
                headers=self.rm_header, 
                data=json.dumps(data))
            if result.json().get('errors'):
                self.api_error_messages_instance += 1
                for error in result.json().get('errors'):
                    timeentry['messages'].extend([f"FAILED TIME POST: {error} ({self.generate_now_string()})" for error in result.json().get('errors')])
                    if error not in self.api_error_messages:
                        self.api_error_messages.append(error)
            return result.status_code == 200
        else:
            # returns false because no proj_id which means could not post. The error was caught and documented in process_timedata_discrepencies()
            return False
        #end region
        #endregion
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
            sheet_sum = grid(sheet_info['ss_sheet_id'])
            sheet_sum.fetch_summary_content()
            self.parent_data= sheet_sum.df.to_dict('records')
            meta_data = {sum_field['title']: sum_field['displayValue'] for sum_field in self.parent_data if sum_field['title'] in ['Project Enumerator [MANUAL ENTRY]', 'DCT Status', 'Build Region', 'Build Job Number', 'Build Architect']}
            sheet_grid = grid(sheet_info['ss_sheet_id'])
            sheet_grid.fetch_content()
            df = sheet_grid.df
            sheet_dict = df[df['Project'].notna()].to_dict('records')
            assignment_data = {}
            for line_item in sheet_dict:
                assignment_data[line_item['Task Name - Primary']] = line_item['Task Status']
            self.ss_proj_list[sheet_i]['meta_data'] = meta_data
            self.ss_proj_list[sheet_i]['assignment_data'] = assignment_data
    def get_rmproj_metadata(self, proj):
        '''checks connected projects for sync of meta data (checking standard, and non standard Arch and Proj Enum fields seperatly), and compares. If out of sync, sounds to api call'''
        endpoint = f"/api/v1/projects/{proj['rm_id']}"
        standard_response = self.paginated_rm_getrequest(endpoint = endpoint)
        custom_response = self.paginated_rm_getrequest(endpoint = endpoint+"/custom_field_values")
    

        if standard_response and custom_response:
            status, status_id, arch, arch_id, enum, enum_id = '', '', '', '', '', ''
            for data_field in custom_response:
                if data_field['custom_field_name'] == "Architect":
                    arch = data_field['value'] 
                    arch_id = data_field['id']
                elif data_field['custom_field_name'] == "Project Enumerator":
                    enum = data_field['value'] 
                    enum_id = data_field['id']
                elif data_field['custom_field_name'] == "DCT Status":
                    status = data_field['value'] 
                    status_id = data_field['id']

            rm_proj_metadata= {
                'job_num':standard_response['project_code'], 
                "region":standard_response['client'],
                "custom_fields":[
                    {'type': 'arch',
                    'value':arch,
                    'rm_id':arch_id},
                    {'type': 'enum',
                    'value':enum,
                    'rm_id':enum_id},
                    {'type': 'status',
                    'value':status,
                    'rm_id':status_id}                   
                ]}
            
            return rm_proj_metadata

        else:
            self.log.log(f"{proj['name']} could not be found on RM")
            return {'message':'error retrieving rm_proj_metadata for updating project meta data'}
        # region updating project meta data
    def execute_conditional_rm_proj_update(self, rm_proj_metadata, proj):
        '''checks for various types of project meta data that has been found to be out of sync.
        standard data fields, tags, and custom data fields each have a different method to update'''
        if proj['meta_data'] == {}:
            self.log.log('Smartsheet meta data is not in Summary names as expected, likely template was note used properly or adjusted')
        if not(rm_proj_metadata['job_num'] == proj['meta_data']['Build Job Number'] and rm_proj_metadata['region'] == proj['meta_data']['Build Region']):
            self. update_rm_proj_standfields(rm_proj_metadata, proj)
        if not(rm_proj_metadata['custom_fields'][0]['value'] == proj['meta_data']['Build Architect'] 
               and rm_proj_metadata['custom_fields'][1]['value'] == proj['meta_data']['Project Enumerator [MANUAL ENTRY]'] 
               and rm_proj_metadata['custom_fields'][2]['value'] == proj['meta_data']['DCT Status']):
            self.update_rm_proj_customfields(rm_proj_metadata, proj)
    def update_rm_proj_standfields(self, rm_proj_metadata, proj):
        '''updates project meta data that has been found to be out of sync.
        standard data fields, tags, and custom data fields each have a different method to update'''
        # standard fields
        data =  {
            'id':proj['rm_id'],
            'project_code':proj['meta_data']['Build Job Number'],
            'client':proj['meta_data']['Build Region'],
        }
        response = requests.put(f"https://api.rm.smartsheet.com/api/v1/projects/{proj['rm_id']}", headers=self.rm_header, data=json.dumps(data))

        if response.status_code == 200:
            self.log.log(f"Updated {proj['name']}'s meta data")
    def update_archived_projects(self):
        '''archived project cannot have a job number // normal name b/c that may interfere with time & expense posting. To do this correctly, I need to first unarchive, then rearchive proj....'''
        self.log.log('Updating Archived Projects as needed...')
        response_dict = self.paginated_rm_getrequest(endpoint='/api/v1/projects?sort_field=created&sort_order=ascending&with_archived=true')
        self.archived_proj = []
        
        for proj in response_dict:
            if proj['archived']:
                self.archived_proj.append(proj)
                if proj['name'].find('ARCHIVED') == -1:
                    self.log.log(f"""{proj['name']} starting update loop
                                 """)
                    data1 =  {
                        'id':proj['id'],
                        'archived':'false'
                    }
                    data2 = {
                        'id':proj['id'],
                        'project_code':" ",
                        'name': f"{proj['name']}_ARCHIVED"
                    }
                    data3 ={
                        'id':proj['id'],
                        'archived':'true'
                    }
                    response1 = requests.put(f"https://api.rm.smartsheet.com/api/v1/projects/{proj['id']}", headers=self.rm_header, data=json.dumps(data1))
                    response2 = requests.put(f"https://api.rm.smartsheet.com/api/v1/projects/{proj['id']}", headers=self.rm_header, data=json.dumps(data2))
                    response3 = requests.put(f"https://api.rm.smartsheet.com/api/v1/projects/{proj['id']}", headers=self.rm_header, data=json.dumps(data3))

                    if response1.status_code and response2.status_code and response3.status_code == 200:
                        self.log.log(f"Correctly Archived {proj['name']}")
                    else:
                        self.log.log(f"error with update- 1:{response1.json()} 2:{response2.json()} 3:{response3.json()}")
        self.grab_rm_projids()
    def update_rm_proj_customfields(self, rm_proj_metadata,proj):
        '''updates project meta data that has been found to be out of sync.
        standard data fields, tags, and custom data fields each have a different method to update'''
        for custom_field in rm_proj_metadata['custom_fields']:
            value = ''
            if custom_field['type'] == 'arch':
                value = proj['meta_data']['Build Architect']
            elif custom_field['type'] == 'enum':
                value = proj['meta_data']['Project Enumerator [MANUAL ENTRY]']
            elif custom_field['type'] == 'status':
                value = proj['meta_data']['DCT Status']
            else:
                self.log.log('failed to post custom field updates, system could not find the fields in its meta data')

            self.response = requests.put(
                f"https://api.rm.smartsheet.com/api/v1/projects/{proj['rm_id']}/custom_field_values/{custom_field['rm_id']}", 
                headers=self.rm_header, 
                data=json.dumps({'value':value}))
            
            if self.response.json().get('message') != "not found":
                self.log.log(f"{proj['name']} updated its custom fields")
            else:
                self.log.log(f"{proj['name']} failed to update its custom fields")
        #endregion
    #endregion
    #region Assignments
    def grab_rm_assignment_data(self, proj):
        '''grabs rm assignment data to check if any updates are needed'''
        rm_assignment_data = self.paginated_rm_getrequest(f"/api/v1/projects/{proj['rm_id']}/assignments")
        rm_assignment_task_to_ids = {}
        for assignment in rm_assignment_data:
            # only adds to list if out of sync
            if self.rm_to_ss_status_ids.get(assignment.get('status_option_id')) != proj['assignment_data'].get(assignment.get('description')):
                rm_assignment_task_to_ids[assignment['description']] = assignment['id']
        proj['rm_assignment_task_to_ids'] = rm_assignment_task_to_ids
    def update_rm_assignments(self, proj):
        '''make updates to assignments, but matching the order in RM to the Order in SS and then mapping SS Task Name/Task Status to RM Assignment Description Work Status'''
        for assignment_key in proj['rm_assignment_task_to_ids']:
            data = {
                'status_option_id': self.ss_to_rm_status_ids[proj['assignment_data'][assignment_key]],
            }
            response = requests.put(f"https://api.rm.smartsheet.com/api/v1/assignments/{proj['rm_assignment_task_to_ids'][assignment_key]}", headers=self.rm_header, data=json.dumps(data))
            if response.status_code == 200:
                self.log.log(f"{proj['name']} successfully updated its task {assignment_key} to a status of {proj['assignment_data'][assignment_key]}")

    #endregion
    #region post to ss
    def post_ss_data(self, data):
        '''posts back to ss a message if the message is different than what is currently there '''
        self.posting_data = []
        for row in data:
            existing_message = str(self.scriptkey_to_script_message[row['key']])
            new_message = " ".join(row['messages'])
            # if the new message and old are the same (barring time-stamp, but not date-stamp), do not update ss
            if existing_message[:len(existing_message)-6] != new_message[:len(new_message)-6]:
                self.posting_data.append({"Script Key":row['key'], 'Script Message':new_message})
        self.posting_data.insert(0, {"Script Key":"EmployeeNumberDateJobApprovalType", 'Script Message':""})
        sheet = grid(self.hh2_data_sheetid)
        sheet.update_rows(self.posting_data, "Script Key")
    #endregion

    def grab_rm_data(self):
        ''''''
        self.log.log("""Grabbing RM Data
                     """)
        self.grab_rm_userids()
        self.audit_users_emplnum()
        self.update_archived_projects()
        self.grab_rm_projids()
    def run_hours_update(self):
        '''runs main script as intended'''
        self.log.log("""Time & Expense Updates:
                     """)
        self.grab_rm_userids()
        self.fetch_and_prepare_hh2_data()
        self.grab_rm_timedata()
        if self.error_w_hh2sheet == []:
            self.process_timedata_discrepencies()
            self.post_rm_time_changes()
            self.post_ss_data(self.flat_hh2_records)
        else:
            self.post_ss_data([{"key":"EmployeeNumberDateJobApprovalType", 'messages':self.error_w_hh2sheet}])
        grid(self.hh2_data_sheetid).handle_update_stamps()
    def run_proj_metadata_update(self):
        '''katherine has mapped particular columns of her project template to meta data fields in RM, this script keeps it up to date'''
        self.log.log("""Project Metadata Updates:
                     """)
        self.grab_proj_sheetids()
        self.establish_sheet_connection()
        tot = len(self.ss_proj_list)
        for proj_i, proj in enumerate(self.ss_proj_list):
            self.log.log(f"{proj_i+1}/{tot}  Assessing {proj['name']}...")
            self.update_sheet_name(proj)
            if proj['status'] == 'connected':
                self.grab_connected_sheet_data(proj_i, proj)
                rm_proj_metadata = self.get_rmproj_metadata(proj)
                try:
                    self.execute_conditional_rm_proj_update(rm_proj_metadata, proj)
                except:
                    self.log.log('issues locating the proj metadata resulted in failed update')
        self.grab_rm_projids()
    def run_assignment_updates(self):
        '''assignments in rm are linked to users and projects and are line-item tasks in ss per project'''
        self.log.log("""Project Assignment Updates:
                     """)
        for proj in self.ss_proj_list:
            if proj['status'] == 'connected':
                self.grab_rm_assignment_data(proj)
                self.update_rm_assignments(proj)

if __name__ == "__main__":
    # https://app.smartsheet.com/sheets/GffHvGGxVJwQ9P8w8gwgfqrmJjcq39JXvMQmH7q1?view=grid is hh2 data sheet
    # https://app.smartsheet.com/browse/workspaces/GXmwRM4wcCmjMVGVjhJ2cWCFR9QWMQCr5w8WGrx1 is proj workspace
    config = {
        'smartsheet_token':smartsheet_token,
        'rm_token':rm_token,
        'hh2_data_sheetid': 1780078719487876,
        'hris_data_sheetid': 5956860349048708,
        'proj_workspace_id': 4883274435716996,
        'proj_list_sheetid': 3858046490306436,
        'rm_to_ss_status_ids':{550725:'Planned', 550729:'Active', 550726:'Potential', 550730:'Completed', 684245:'Check-in', 684246:'Not Completed', 698235:'Blocked'},
        'ss_to_rm_status_ids':{'Planned': 550725, 'Active':550729, 'Potential':550726, 'Completed':550730, 'Check-in':684245, 'Not Completed':684246, 'Blocked':698235}
        
    }
    sra = SmartsheetRmAdmin(config)
    sra.grab_rm_data()
    sra.run_proj_metadata_update()
    sra.run_hours_update()
    sra.run_assignment_updates()
    sra.log.log("""~Fin
                     
                """)