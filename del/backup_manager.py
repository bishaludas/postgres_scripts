import psycopg2
import os
import json
import pandas as pd
import traceback
import pdb

class BackupManager:

    __config = None
    __connection = None

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
    
    # TODO Encapsulate connection 
    def get_db_connection(self):
        """Returns a valid database connection"""
        db_config = self.__config.get('database')
        try:
            if self.__connection is None:
                # Create a new connection is not already created
                self.__connection = psycopg2.connect(host=db_config.get('db_host'),
                                                     port=db_config.get('db_port'),
                                                     user=db_config.get('db_user'),
                                                     password=db_config.get('db_pword'),
                                                     dbname=db_config.get('db_database'))
        except Exception as e:
            raise Exception(e)
        return self.__connection

    # TODO Encapsulate execute query 
    def get_query_result(self, query, data = None):
        """
            Returns results of query provided
            INPUT: query to be executed
            OUTPUT: result of the query
        """

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            print('Executing query: ')
            # print(query)
            cursor.execute(query, data)
            print(cursor.query)
            columns = cursor.description
            result = [{columns[index][0]:column for
                  index, column in enumerate(value)}
                 for value in cursor.fetchall()]
            return result
        except Exception as e:
            raise Exception(e)

    def get_user_ids(self, email_ids):
        """
            Returns user_ids of all the users that matches the email id list.
            Input: list of email ids
        """
        fetch_user_id_query = """select id from users where email in %(emails)s"""
        email_tuple = tuple(email_ids)

        result = self.get_query_result(fetch_user_id_query, {'emails' : email_tuple})
        result = pd.DataFrame(result)

        user_ids = result['id'].tolist()

        return user_ids

    def backup_user_data(self, user_ids):
        """
            Returns user data of all the users that matches the user ids list.
            Input: list of user ids
        """
        try:
            fetch_user_data_query = """select u.*, o.orgid
                from users u 
                inner join organizations o on u.organization_id = o.id 
                where u.id in %(user_ids)s """

            # user_id_list = ', '.join('{}'.format(id) for id in user_ids)
            # user_data_fetch_query = fetch_user_data_query % user_id_list

            user_id_tuple = tuple(user_ids)
            result = self.get_query_result(fetch_user_data_query, {'user_ids':user_id_tuple})

            if len(result) > 0:
                user_data_df = pd.DataFrame(result)
                
                user_data_df = user_data_df.set_index('id')

                user_data = user_data_df.to_json(orient='index')
                print('Backing up users data. %s entries found.' % len(user_data_df)  )
                print('\n')
                with open('backups/user_details_data.json', 'w') as outfile:
                    outfile.write(user_data)

        except Exception as e:
            print("Error occured while fetching users details.")
            raise Exception(e)


    def backup_user_roles(self, user_ids):
        """
            Returns users roles data of all the users that matches the user ids list.
            Input: list of user ids
        """
        try:
            
            fetch_user_roles_query = """ select ru.user_id, t.roles_t as roles from roles_users ru 
                join 
                    ( select r.id,
                        json_build_object('name', r.name, 'description', r.description
                        ,'organization_id', r.organization_id, 'orgid', o.orgid) as roles_t 
                    from roles r 
                    join organizations o on r.organization_id=o.id
                ) t on t.id=ru.role_id where ru.user_id in %(user_ids)s """

            # user_id_list = ', '.join('{}'.format(id) for id in user_ids)
            # user_roles_fetch_query = fetch_user_roles_query % user_id_list
            
            user_id_tuple = tuple(user_ids)
            result = self.get_query_result(fetch_user_roles_query, {'user_ids':user_id_tuple})
            if len(result) > 0:
                user_roles_df = pd.DataFrame(result)
                user_roles_df = user_roles_df.set_index('user_id')

                user_role_map = (dict(zip(user_roles_df.index,user_roles_df.roles)))
                new_user_roles_df=pd.DataFrame(user_role_map).T

                users_roles = new_user_roles_df.to_json(orient = 'index')

                print('Backing up user roles data. %s entries found.' % len(new_user_roles_df) )
                print('\n')
                with open('backups/user_roles_data.json', 'w') as outfile:
                    outfile.write(users_roles)

        except Exception as e:
            print("Error occured while fetching users roles.")
            raise Exception(e)

    def backup_user_keys(self, user_ids):
        """
            Returns user keys data of all the users that matches the user ids list.
            Input: list of user ids
        """
        try:
            fetch_user_keys_query = """select * from users_keys where user_id in %(user_ids)s"""
            # user_id_list = ', '.join('{}'.format(id) for id in user_ids)
            # user_keys_fetch_query = fetch_user_keys_query % user_id_list
            
            user_id_tuple = tuple(user_ids)
            result = self.get_query_result(fetch_user_keys_query, {'user_ids':user_id_tuple})

            if len(result) > 0:
                user_keys_df = pd.DataFrame(result)
                user_keys_df = user_keys_df.set_index('user_id')
                user_data = user_keys_df.to_json(orient='index')
                
                print('Backing up user keys data. %s entries found.' % len(user_keys_df) )
                print('\n')
                with open('backups/user_keys_data.json', 'w') as outfile:
                    outfile.write(user_data)

        except Exception as e:
            print("Error occured while fetching users keys data.")
            raise Exception(e)
    

    def resource_settings(self, user_ids):
        try:
            fetch_resource_settings_query = """ select * from resource_settings where resource_type='USER' and resource_id in %(user_ids)s"""
            user_id_tuple = tuple(user_ids)

            # user_id_list = ', '.join('{}'.format(id) for id in user_ids)
            # resource_settings_fetch_query = fetch_resource_settings_query % user_id_list

            result = self.get_query_result(fetch_resource_settings_query, {'user_ids':user_id_tuple})
            if len(result) < 0:
                print("Failed to fetch resource settings.")
                raise Exception("Failed to fetch resource settings.")

            resource_settings_df = pd.DataFrame(result)
            resource_settings_df['col1'] = resource_settings_df[['resource_type', 'settings','description']].to_dict(orient='record')
            list_of_df=[]

            for idx, grp in resource_settings_df.groupby('resource_id'):
                grp['col2']=[dict(zip(grp.setting_group, grp.col1))] * len(grp)
                list_of_df.append(grp)

            result_df = pd.concat(list_of_df).drop_duplicates('resource_id')
            result_df = result_df[['resource_id','col2']]
            result_data = json.dumps(dict(zip(result_df.resource_id,result_df.col2)))
            
            print('Backing up resource settings data. %s entries found.' % len(result_df) )
            print('\n')
            with open('backups/resource_settings.json', 'w') as outfile:
                outfile.write(result_data)
            
        except Exception as e:
            print("Error occured while fetching resource settings data.")
            raise Exception(e) 


    def backup_data(self):
        """
            Reads the users email id list from the file and creates backup as required
        """
        try:
            retain_list = []
            with open('retain_list.txt', 'r') as f:
                retain_list = f.readlines()
            retain_list = list(map(lambda x: x.strip(), retain_list))

            user_ids = self.get_user_ids(retain_list)

            self.backup_user_data(user_ids)
            self.backup_user_roles(user_ids)
            self.backup_user_keys(user_ids)
            self.resource_settings(user_ids)

        except Exception as e:
            print("Error occurred while processing backup. ")
            traceback.print_exc()
    

if __name__ == '__main__':
    manager = BackupManager()

    manager.backup_data()