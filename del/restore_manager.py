import psycopg2
import os
import pandas as pd
import numpy as np
import json
import datetime
import pdb

class RestoreManager:
    __config = None
    __connection = None
    backup_path = "backups/"

    def __init__(self, config_file=None):
        if config_file is None:
            config_file_path = os.path.join(os.path.dirname(__file__), "config", "config.json")
        else:
            config_file_path = config_file

        if not os.path.exists(config_file_path):
            raise Exception("Configuration file {0} does not exist".format(config_file_path))

        try:
            with open(config_file_path) as config_json:
                self.__config = json.load(config_json)
        except Exception as e:
            raise Exception(e)

    def get_db_connection(self):
        """Returns a valid database connection"""
        db_config = self.__config.get('database')
        try:
            if self.__connection is None:
                # Create a new connection if not already created
                self.__connection = psycopg2.connect(host=db_config.get('db_host'),
                                                     port=db_config.get('db_port'),
                                                     user=db_config.get('db_user'),
                                                     password=db_config.get('db_pword'),
                                                     dbname=db_config.get('db_database'))
                # print('DB connection established.')
        except Exception as e:
            raise Exception(e)
        return self.__connection

    
    def get_query_result(self, query, data=None):
        """
            Returns results of query provided
            
            Arguments:
                arg1: query
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            # print('Executing query: ')
            # print(query)
            cursor.execute(query, data)
            columns = cursor.description
            result = [{columns[index][0]:column for
                  index, column in enumerate(value)}
                 for value in cursor.fetchall()]
            
            return result
        except Exception as e:
            raise Exception(e)

    # TODO move to DButills
    def insert_to_DB(self, query, values = None):
        """ 
            Inserts to Database
            Arguments:
                arg1: query
                value: Secondary may be none or column values 
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit() 

            columns = cursor.description
            if columns is not None:
                result = cursor.fetchall()
                return result
            
        except Exception as e:
            print("Cannot insert to DB", e)

    def get_last_insert_id(self, result_list):
        """
            Fetches id of last inserted item 
        
            Arguments
                arg1 : last inserted item list
        
            Result : returns id or None
        """
        try:
            if (len(result_list) > 0):
                return result_list[0][0]
            else:
                return None
        except Exception as e:
            print("Sorry the id could not be fetched. ", e)


    def check_if_orgid_exist(self, orgid):
        """
            Checks if organization with orgid exists.
        
            Arguments
                arg1 : orgid
        
            Result : Returns true if exists , else fail
        """
        try:
            __fetch_organization_query = """ SELECT id from organizations where orgid = %(org_id)s """
            data = { 'org_id': orgid}
            result = self.get_query_result(__fetch_organization_query, data)

            if(len(result) > 0 ):
                return True
            else:
                return False
        except Exception as e:
            print(e)

    def check_if_user_exist(self, email, organization_id):
        """
            Checks if user with email and organization_id exists
        
            Arguments
                arg1 : user email
                arg2 : organization id
        
            Result : Returns true if exists, else false
        """
        try:
            __fetch_user_query = """select id from users where email= %(email)s and organization_id= %(organization_id)s"""
            data = {'email': email, 'organization_id': organization_id}
            result = self.get_query_result(__fetch_user_query, data)
                    
            if(len(result) > 0 ):
                return True
            else:
                return False
        except Exception as e:
            print(e)

    def restore_user_details(self, user_details_df):
        """
            Restores the details of user.  
        
            Arguments
                arg1 : user details dataframe
        
            Result : Newly restored users id
        """
        
        date_columns = ['created_at', 'current_sign_in_at', 'invitation_accepted_at', 'invitation_created_at', 'invitation_sent_at', 'last_sign_in_at', 'password_changed_at', 'remember_created_at', 'reset_password_sent_at', 'updated_at']
        
        for date in date_columns: 
            user_details_df[date] = pd.to_datetime(user_details_df[date] , unit='ms').astype(str)
            user_details_df[date] = user_details_df[date].apply(lambda x: None if x=='NaT' else x)
            
        user_details_df = user_details_df.reset_index().T.drop(['index'])
        user_details_dict = user_details_df.to_dict()

        restore_query = """INSERT INTO users (created_at, current_sign_in_at,current_sign_in_ip,  email, encrypted_password, failed_attempts, invitation_accepted_at, invitation_created_at, invitation_limit, invitation_sent_at, invitation_token, invitations_count, invited_by_id, invited_by_type, is_mirrored_user, last_sign_in_at, last_sign_in_ip, locked_at, name, organization_id, password_changed_at, remember_created_at, reset_password_sent_at, reset_password_token, sign_in_count, status, unlock_token, updated_at) VALUES 
        (%(created_at)s,%(current_sign_in_at)s,%(current_sign_in_ip)s, %(email)s, %(encrypted_password)s, %(failed_attempts)s, %(invitation_accepted_at)s,%(invitation_created_at)s,%(invitation_limit)s,%(invitation_sent_at)s,%(invitation_token)s,%(invitations_count)s,%(invited_by_id)s,%(invited_by_type)s,%(is_mirrored_user)s,%(last_sign_in_at)s,%(last_sign_in_ip)s,%(locked_at)s,%(name)s,%(organization_id)s,%(password_changed_at)s,%(remember_created_at)s,%(reset_password_sent_at)s,%(reset_password_token)s,%(sign_in_count)s,%(status)s,%(unlock_token)s,%(updated_at)s) RETURNING id"""
        
        try:
            result = self.insert_to_DB(restore_query, user_details_dict[0])
        except Exception as e:
            print("Cannot insert user Data : ", e)

        return self.get_last_insert_id(result)


    def restore_user_keys(self, user_keys_data, new_user_id):
        """
            Restores the user_keys table data. 
        
            Arguments
                arg1 : user keys dataframe
                arg2 : newly inserted user id
        """
        user_keys_list = []
        user_keys_dict = user_keys_data
        user_keys_list.append(user_keys_data)
        user_keys_df = pd.DataFrame(user_keys_list)
        user_keys_df['user_id'] = new_user_id

        restore_user_keys_query = """INSERT INTO users_keys (api_key, generated_count, jwt_token, status, user_id) VALUES (%(api_key)s,%(generated_count)s, %(jwt_token)s, %(status)s, %(user_id)s) """
        
        # Not consistent check if T is required or not
        user_keys_df = user_keys_df.T
        user_keys_dict = user_keys_df.to_dict()

        try:
            self.insert_to_DB(restore_user_keys_query, user_keys_dict[0])
        except Exception as e:
            print("Cannot restore user key : ", e)


    def restore_user_role(self, user_role_data, new_user_id):
        """
            Checks if role exist -> return id ->insert to roles_user \n
            Or Insert role -> return id -> insert to roles_user

            Arguments
                arg1 : user role dict data
                arg2 : newly inserted user id

            output : inserts data into users_role table
        """
        
        name = user_role_data['name']
        organization_id = user_role_data['organization_id']
        orgid = user_role_data['orgid']
        description = user_role_data['description']

        check_role_query = """ select r.id from roles r join organizations o on r.organization_id=o.id where r.name= %(name)s and o.orgid= %(orgid)s and r.organization_id= %(organization_id)s """
        result =  self.get_query_result(check_role_query, {'name': name,'orgid': orgid,'organization_id': organization_id})
        
        restore_user_role_query = """INSERT INTO roles_users (role_id, user_id) VALUES (%(role_id)s, %(user_id)s) """
        
        try:
            if len(result) > 0:
                data = {'role_id':result[0]['id'], 'user_id':new_user_id}
                self.insert_to_DB(restore_user_role_query, data)                
            else:
                # insert into roles
                insert_to_roles_query = """ INSERT INTO roles (name, description, organization_id, created_at, updated_at) VALUES 
                (%(name)s, %(description)s, %(organization_id)s, %(created_at)s, %(updated_at)s) RETURNING id"""
                
                date_now = datetime.datetime.now()
                role_dict = {'name':name,'description':description, 'organization_id':organization_id, 'created_at': date_now, 'updated_at':date_now}
                role_data = self.insert_to_DB(insert_to_roles_query, role_dict)
                print('Role inserted to DB')
                
                new_role_id = self.get_last_insert_id(role_data)
                user_role_dict = {'role_id':new_role_id, 'user_id':new_user_id}
                self.insert_to_DB(restore_user_role_query, user_role_dict)

        except Exception as e:
            print("Cannot restore user roles : ", e)


    def restore_resource_settings(self, resource_settings_data, new_user_id):
        """
            Restores the resource_settings table data.
        
            Arguments
                arg1 : resource setting dict 
                arg2 : newly inserted user id
        """
        new_data={}
        resource_settings_query = """ INSERT INTO resource_settings (resource_type, resource_id, setting_group, settings, description) VALUES (%(resource_type)s, %(resource_id)s, %(setting_group)s, %(settings)s, %(description)s)"""
        for key in resource_settings_data:
            new_data['resource_id'] = new_user_id
            new_data['setting_group'] =key 
            new_data['resource_type'] = resource_settings_data[key]['resource_type']
            new_data['settings'] = json.dumps(resource_settings_data[key]['settings'])
            new_data['description'] = resource_settings_data[key]['description']

            try:            
                self.insert_to_DB(resource_settings_query, new_data)
            except Exception as e:
                print("Cannot restore Resource Settings :", e)

    def restore_data(self):
        """
            Reads the backup json file and restores the data on tables.
        """
        try:
            # TODO : Generalization into sub function
            with open(self.backup_path +'user_details_data.json') as f:
                user_details = json.load(f)
            
            with open(self.backup_path +'user_keys_data.json') as f:
                user_keys = json.load(f)

            with open(self.backup_path +'user_roles_data.json') as f:
                users_roles = json.load(f)
            
            with open(self.backup_path +'resource_settings.json') as f:
                resource_settings = json.load(f)
        except Exception as e:
                print("Error occurred while reading json file data.", e)

        import pdb; pdb.set_trace()
        # user_details_df
        user_details_df = pd.DataFrame(user_details).T
        
        try:
            for key,value in user_details_df.iterrows():
                # TODO key = old_user_id
                if(self.check_if_orgid_exist(value.orgid) == False):
                    print(' The organization with orgid: ', value.orgid, 'does not exist.', '\n')
                    continue
                
                if(self.check_if_user_exist(value.email, value.organization_id) == True):
                    print("The user with Email: ",value.email,"and organisation id ",value.organization_id ," already Exist")
                    continue

                # TODO already in a loop, might not need to use loc
                user_details_data = user_details_df.loc[key]
                new_user_details_df = pd.DataFrame(user_details_data).T
                new_user_details_df = new_user_details_df.drop(columns=['orgid'])

                new_user_id = self.restore_user_details(new_user_details_df)
                print(value.email,' with organization id ',value.organization_id, 'was restored')
                
                if new_user_id is None:
                    # TODO : alert user couldn't be inserted
                    continue

                # TODO : try to use a single wrapper function for below actions:
                # user keys
                user_keys_data = user_keys.get(key)
                if user_keys_data is None:               
                    print('No user keys found.')
                
                self.restore_user_keys(user_keys_data, new_user_id)
                print('User key restored.')
                
                #user roles    
                user_role_data = users_roles.get(key)
                if user_role_data is None:
                    print('User does not have any role.')
                
                self.restore_user_role(user_role_data, new_user_id)
                print("User role restored")

                # resource settings
                resource_settings_data = resource_settings.get(key)    
                if resource_settings_data is not None:
                    print('Resource settings not found.')
                
                self.restore_resource_settings(resource_settings_data, new_user_id)
                print('Resource settings restored.')
        except Exception as e:
            print("Cannot restore user Data", e)


if __name__ == '__main__':
    manager = RestoreManager()

    manager.restore_data()