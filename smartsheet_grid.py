#!/usr/bin/env python

import smartsheet
import pandas as pd
import datetime
import time
import math

class grid:
    """
    A class that interacts with Smartsheet using its API.

    This class provides functionalities such as fetching sheet content, 
    and posting new rows to a given Smartsheet sheet.

    Important:
    ----------
    Before using this class, the 'token' class attribute should be set 
    to the SMARTSHEET_ACCESS_TOKEN.

    Attributes:
    -----------
    token : str, optional
        The access token for Smartsheet API.
    grid_id : int
        ID of an existing Smartsheet sheet.
    grid_content : dict, optional
        Content of the sheet fetched from Smartsheet as a dictionary.

    Methods:
    --------
    get_column_df() -> DataFrame:
        Returns a DataFrame with details about the columns, such as title, type, options, etc.

    fetch_content() -> None:
        Fetches the sheet content from Smartsheet and sets various attributes like columns, rows, row IDs, etc.

    fetch_summary_content() -> None:
        Fetches and constructs a summary DataFrame for summary columns.

    reduce_columns(exclusion_string: str) -> None:
        Removes columns from the 'column_df' attribute based on characters/symbols provided in the exclusion_string.

    grab_posting_column_ids(filtered_column_title_list: Union[str, List[str]]="all_columns") -> None:
        Prepares a dictionary for column IDs based on their titles. Used internally for posting new rows.

    delete_all_rows() -> None:
        Deletes all rows in the current sheet.

    post_new_rows(posting_data: List[Dict[str, Any]], post_fresh: bool=False, post_to_top: bool=False) -> None:
        Posts new rows to the Smartsheet. Can optionally delete the whole sheet before posting or set the position of the new rows.

    update_rows(posting_data: List[Dict[str, Any]], primary_key: str):
        Updates rows that can be updated, posts rows that do not map to the sheet.

    grab_posting_row_ids(posting_data: List[Dict[str, Any]], primary_key: str):
        returns a new posting_data called update_data that is a dictionary whose key is the row id, and whose value is the dictionary for the row <column name>:<field value>

    
    Dependencies:
    -------------
    - smartsheet (from smartsheet-python-sdk)
    - pandas as pd
    """

    token = None

    def __init__(self, grid_id):
        self.grid_id = grid_id
        self.grid_content = None
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.smart = smartsheet.Smartsheet(access_token=self.token)
            self.smart.errors_as_exceptions(True)
#region core get requests   
    def get_column_df(self):
        '''returns a df with data on the columns: title, type, options, etc...'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            return pd.DataFrame.from_dict(
                (self.smart.Sheets.get_columns(
                    self.grid_id, 
                    level=2, 
                    include='objectValue', 
                    include_all=True)
                ).to_dict().get("data"))
    def fetch_content(self):
        '''this fetches data, ask coby why this is seperated
        when this is done, there are now new objects created for various scenarios-- column_ids, row_ids, and the main sheet df'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet(self.grid_id)).to_dict()
            self.grid_name = (self.grid_content).get("name")
            self.grid_url = (self.grid_content).get("permalink")
            # this attributes pulls the column headers
            self.grid_columns = [i.get("title") for i in (self.grid_content).get("columns")]
            # note that the grid_rows is equivelant to the cell's 'Display Value'
            self.grid_rows = []
            if (self.grid_content).get("rows") == None:
                self.grid_rows = []
            else:
                for i in (self.grid_content).get("rows"):
                    b = i.get("cells")
                    c = []
                    for i in b:
                        l = i.get("displayValue")
                        m = i.get("value")
                        if l == None:
                            c.append(m)
                        else:
                            c.append(l)
                    (self.grid_rows).append(c)
            
            # resulting fetched content
            self.grid_rows = self.grid_rows
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("rows")]
            self.grid_column_ids = [i.get("id") for i in (self.grid_content).get("columns")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.grid_columns)
            # Should be row_id intead of id as that is less likely to be taken name space!!!
            self.df["id"]=self.grid_row_ids
            self.column_df = self.get_column_df()
    def fetch_summary_content(self):
        '''builds the summary df for summary columns'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet_summary_fields(self.grid_id)).to_dict()
            # this attributes pulls the column headers
            self.summary_params=['title','createdAt', 'createdBy', 'displayValue', 'formula', 'id', 'index', 'locked', 'lockedForUser', 'modifiedAt', 'modifiedBy', 'objectValue', 'type']
            self.grid_rows = []
            if (self.grid_content).get("data") == None:
                self.grid_rows = []
            else:
                for summary_field in (self.grid_content).get("data"):
                    row = []
                    for param in self.summary_params:
                        row_value = summary_field.get(param)
                        row.append(row_value)
                    self.grid_rows.append(row)
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("data")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.summary_params)
#endregion 
#region helpers     
    def reduce_columns(self,exclusion_string):
        """a method on a grid{sheet_id}) object
        take in symbols/characters, reduces the columns in df that contain those symbols"""
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            regex_string = f'[{exclusion_string}]'
            self.column_reduction =  self.column_df[self.column_df['title'].str.contains(regex_string,regex=True)==False]
            self.reduced_column_ids = list(self.column_reduction.id)
            self.reduced_column_names = list(self.column_reduction.title)
#endregion
#region ss post
    #region new row(s)
    def grab_posting_column_ids(self, filtered_column_title_list="all_columns"):
        '''preps for ss post 
        creating a dictionary per column:
        { <title of column> : <column id> }
        filtered column title list is a list of column title str to prep for posting (if you are not posting to all columns)
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''

        column_df = self.get_column_df()

        if filtered_column_title_list == "all_columns":
            filtered_column_title_list = column_df['title'].tolist()
    
        self.column_id_dict = {title: column_df.loc[column_df['title'] == title]['id'].tolist()[0] for title in filtered_column_title_list}
    def delete_all_rows(self):
        '''deletes up to 400 rows in 200 row chunks by grabbing row ids and deleting them one at a time in a for loop
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''
        self.fetch_content()

        row_list_del = []
        for rowid in self.df['id'].to_list():
            row_list_del.append(rowid)
            # Delete rows to sheet by chunks of 200
            if len(row_list_del) > 199:
                self.smart.Sheets.delete_rows(self.grid_id, row_list_del)
                row_list_del = []
        # Delete remaining rows
        if len(row_list_del) > 0:
            self.smart.Sheets.delete_rows(self.grid_id, row_list_del) 
    def post_new_rows(self, posting_data, post_fresh = False, post_to_top=False):
        '''posts new row to sheet, does not account for various column types at the moment
        posting data is a list of dictionaries, one per row, where the key is the name of the column, and the value is the value you want to post
        then this function creates a second dictionary holding each column's id, and then posts the data one dictionary at a time (each is a row)
        post_to_top = the new row will appear on top, else it will appear on bottom
        post_fresh = first delete the whole sheet, then post (else it will just update existing sheet)
        TODO: if using post_to_top==False, I should really delete the empty rows in the sheet so it will properly post to bottom'''
        
        posting_sheet_id = self.grid_id
        column_title_list = list(posting_data[0].keys())
        try:
            self.grab_posting_column_ids(column_title_list)
        except IndexError:
            raise ValueError("Index Error reveals that your posting_data dictionary has key(s) that don't match the column names on the Smartsheet")
        if post_fresh:
            self.delete_all_rows()
        
        rows = []

        for item in posting_data:
            row = smartsheet.models.Row()
            row.to_top = post_to_top
            row.to_bottom= not(post_to_top)
            for key in self.column_id_dict:
                if item.get(key) != None:     
                    row.cells.append({
                    'column_id': self.column_id_dict[key],
                    'value': item[key]
                    })
            rows.append(row)

        self.post_response = self.smart.Sheets.add_rows(posting_sheet_id, rows)
    #endregion
    #region post timestamp
    def handle_update_stamps(self):
        '''grabs summary id, and then runs the function that posts the date'''
        current_date = datetime.date.today()
        formatted_date = current_date.strftime('%m/%d/%y')

        sum_id = self.grabrcreate_sum_id("Last API Automation", "DATE")
        self.post_to_summary_field(sum_id, formatted_date)
    def grabrcreate_sum_id(self, field_name_str, sum_type):
        '''checks if there is a DATE summary field called "Last API Automation", if Y, pulls id, if N, creates the field.
        then posts today's date to that field
        [ONLY TESTED FOR DATE FIELDS FOR NOW]'''
        # First, let's fetch the current summary fields of the sheet
        self.fetch_summary_content()

        # Check if "Last API Automation" summary field exists
        automation_field = self.df[self.df['title'] == field_name_str]

        # If it doesn't exist, create it
        if automation_field.empty:
            new_field = smartsheet.models.SummaryField({
                "title": field_name_str,
                "type": sum_type
            })
            response = self.smart.Sheets.add_sheet_summary_fields(self.grid_id, [new_field])
            # Assuming the response has the created field's data, extract its ID
            self.sum_id = response.data[0].id
        else:
            # Extract the ID from the existing field
            self.sum_id = automation_field['id'].values[0]

        return self.sum_id
    def post_to_summary_field(self, sum_id, post):
        '''posts to sum field, 
        designed to: posts date to summary column to tell ppl when the last time this script succeeded was
        [ONLY TESTED FOR DATE FIELDS FOR NOW]'''

        sum = smartsheet.models.SummaryField({
            "id": int(sum_id),
            "ObjectValue": post
        })
        resp = self.smart.Sheets.update_sheet_summary_fields(
            self.grid_id,    # sheet_id
            [sum],
            False    # rename_if_conflict
        )
    #endregion
    #region post row update
    def grab_posting_row_ids(self, posting_data, primary_key, skip_nonmatch=False):
        '''Prepares for an update by reorganizing the posting data with the row_id as the key and the value as the data.    

        Parameters:
        - posting_data: Dictionary where each key is a column name and each value is the corresponding row value for that column.
        - primary_key: A key from `posting_data` that serves as the reference to map row IDs to the posting data (must be case-sensitive match). 
            In otherwords, the primary_key is a str that matches one of the keys from the posting_data. This key represents the column that will be used to extract Row_IDs by finding the first row to match each posting_data's primary key value, and calling that the row Id for that dictionary
        - skip_nonmatch (optional, default=True): Determines the handling of non-matching primary keys. When set to `True`, rows with non-matching primary keys are ignored. When `False`, these rows are collected into a "new_rows" key in the resulting dictionary.  

        Process:
        1. Identify the value associated with the `primary_key` in `posting_data`.
        2. Search for this value in the Smartsheet to find its row_id.
        3. Return a dictionary: keys are row_ids (or "new_rows" for unmatched rows), values are the corresponding `posting_data` for each row.
        '''

        self.fetch_content()

        if not self.df.empty:
            # Mapping of the primary key values to their corresponding row IDs from the current Smartsheet data
            primary_to_row_id = dict(zip(self.df[primary_key], self.df['id']))  

            # Dictionary to hold the mapping of row IDs to their posting data
            update_data = {}
            new_rows = []   

            for data in posting_data:
                primary_value = data.get(primary_key)
                if primary_value in primary_to_row_id:
                    row_id = primary_to_row_id[primary_value]
                    update_data[row_id] = data
                elif not skip_nonmatch:
                    new_rows.append(data)   

            if new_rows:
                update_data['new_rows'] = new_rows  

            # Check if there were no matches at all
            if not update_data:
                raise ValueError(f"The primary_key '{primary_key}' had no matches in the current Smartsheet data.") 

            return update_data
        else:
            raise ValueError("Grid Instance is not appropriate for this task. Try create a new grid instance")
    def update_rows(self, posting_data, primary_key, update_type='default'):
        '''
        Updates rows (and adds misc rows) in the Smartsheet based on the provided posting data.  

        Parameters:
        - posting_data (list of dicts)
        - primary_key (string which is equal to a key of one of the items in all dictionaries)

        Returns:
        None. Updates and possibly adds rows in the Smartsheet.
        '''
        posting_sheet_id = self.grid_id
        column_title_list = list(posting_data[0].keys())
        try:
            self.grab_posting_column_ids(column_title_list)
        except IndexError:
            raise ValueError("Index Error reveals that your posting_data dictionary has key(s) that don't match the column names on the Smartsheet")
        self.update_data = self.grab_posting_row_ids(posting_data, primary_key)

        if update_type =='debug':
            # Handle existing rows' updates (printing each row)
            for i, row_id in enumerate(self.update_data.keys()):
                if row_id != "new_rows":
                    # Build the row to update
                    new_row = smartsheet.models.Row()
                    new_row.id = row_id
                    for column_name in self.column_id_dict.keys():
                        # does not post repost primary key
                        if column_name != primary_key:
                            # Build new cell value
                            new_cell = smartsheet.models.Cell()
                            new_cell.column_id = int(self.column_id_dict[column_name])
                            # stops error where post doesnt go through because value is "None"
                            if self.update_data[row_id].get(column_name) != None:
                                print(f"{i+1}/{len(self.update_data.keys())}  ", self.update_data[row_id].get(column_name))
                                new_cell.value = self.update_data[row_id].get(column_name)
                            else:
                                new_cell.value = ""
                            new_cell.strict = False
                            new_row.cells.append(new_cell)

                    # Update rows
                    self.update_response = self.smart.Sheets.update_rows(
                      posting_sheet_id ,      # sheet_id
                      [new_row])
                    
        elif update_type == 'batch':
            rows = []
            counter = 1
            batch_total = int(math.ceil(len(self.update_data.keys()) / 350))
            self.update_response = []       

            for i, row_id in enumerate(self.update_data.keys()):
                if row_id != "new_rows":
                    new_row = smartsheet.models.Row()
                    new_row.id = row_id
                    for column_name in self.column_id_dict.keys():
                        if column_name != primary_key:
                            new_cell = smartsheet.models.Cell()
                            new_cell.column_id = int(self.column_id_dict[column_name])
                            new_cell.value = self.update_data[row_id].get(column_name, "")  # Use get method to handle None
                            new_cell.strict = False
                            new_row.cells.append(new_cell)
                    rows.append(new_row)  # Properly add the new_row to the rows list       

                # When 350 rows are collected or at the end of the data
                if (i + 1) % 350 == 0 or (i + 1) == len(self.update_data.keys()):
                    # Send the batch update
                    self.update_response.append(self.smart.Sheets.update_rows(
                        posting_sheet_id,
                        rows  # Now passing the entire list of rows
                    ))
                    print(f"Batch {counter}/{batch_total}: updated {i + 1}/{len(self.update_data.keys())} in smartsheet")
                    time.sleep(2)  # Optional: sleep to avoid hitting rate limits or as needed
                    counter += 1
                    rows = []  # Reset the rows list for the next batch     

            # After the loop, check if there's any leftover rows to update
            if rows:
                self.update_response.append(self.smart.Sheets.update_rows(
                    posting_sheet_id,
                    rows
                ))
                print(f"Final batch: updated remaining {len(rows)} rows in smartsheet")

        
        elif update_type == 'default':
            rows = []
            # Handle existing rows' updates
            for row_id in self.update_data.keys():
                if row_id != "new_rows":
                    # Build the row to update
                    new_row = smartsheet.models.Row()
                    new_row.id = row_id
                    for column_name in self.column_id_dict.keys():
                        # does not post repost primary key
                        if column_name != primary_key:
                            # Build new cell value
                            new_cell = smartsheet.models.Cell()
                            new_cell.column_id = int(self.column_id_dict[column_name])
                            # stops error where post doesnt go through because value is "None"
                            if self.update_data[row_id].get(column_name) != None:
                                new_cell.value = self.update_data[row_id].get(column_name)
                            else:
                                new_cell.value = ""
                            new_cell.strict = False
                            new_row.cells.append(new_cell)
                    rows.append(new_row)

            # Update rows
            self.update_response = self.smart.Sheets.update_rows(
              posting_sheet_id ,      # sheet_id
              rows)

        try:
            # Handle addition of new rows if the "new_rows" key is present
            self.post_new_rows(self.update_data.get('new_rows'))
        except TypeError:
            pass
    #endregion
#endregion