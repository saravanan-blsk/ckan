import ckan.plugins.toolkit as toolkit
import db


class ColumnNameMapping:
    """Create data store table for mapping lengthy column names."""

    @staticmethod
    def map_column_name(data_dict):
        """Map lengthy column names

        :param data_dict: dict, dictionary of resource data
        :return: dict, resourced data with formatted column names
        """
        mapped_columns = {}
        truncated_columns = {}
        counter = 1
        for column in data_dict['fields']:
            if column['id'] is not None and column['id'] is not '':
                if len(column['id']) > 60:
                    original_name = column['id']
                    mapped_name, truncated_columns = ColumnNameMapping.create_truncated_name(original_name,
                                                                                             truncated_columns)
                    column['id'] = mapped_name
                    counter += 1
                    mapped_columns[mapped_name] = original_name
                    for row in data_dict['records']:
                        dict_index = data_dict['records'].index(row)
                        row[mapped_name] = row.pop(original_name)
                        data_dict['records'][dict_index] = row
        return data_dict, mapped_columns

    @staticmethod
    def create_mapping_table(context, data_dict, mapped_columns):
        """Create table to store the metadata of mapped column names

        :param mapped_columns: dict, mapped column names
        :param context: context
        :param data_dict: data_dict
        """
        datastore_dict = {}

        # Creating name for the mapping datastore
        try:
            dataset_name = data_dict.get('resource').get('name')
            data_dict['resource']['name'] = dataset_name + '_mapping'


        except Exception:
            resource_data = toolkit.get_action('resource_show')(
                context, {'id': data_dict.get('resource_id')})
            # print "----------------------resource_data--------------------------------"
            # print resource_data
            # print "------------------------------------------------------"
            temp_name = resource_data.get('name')
            dataset_name = temp_name if temp_name is not None else resource_data.get('description')
            data_dict.update({'resource': {}})
            data_dict['resource'].update({'name': dataset_name + '_mapping'})
            data_dict['resource'].update({'package_id': resource_data.get('package_id')})

        print "Checking if the resource exists"
        if ColumnNameMapping.check_resource_list(context, data_dict):
            return

        resource_dict = toolkit.get_action('resource_create')(
            context, data_dict['resource'])
        resource_id = resource_dict['id']
        # package_id = data_dict['resource']['package_id']
        print "Mapping Created:::::", resource_id
        datastore_dict['connection_url'] = data_dict['connection_url']
        datastore_dict['resource_id'] = str(resource_id)
        fields = [{'id': 'mapped_column', 'type': 'text'},
                  {'id': 'original_name', 'type': 'text'},
                  {'id': 'mapping_id', 'type': 'text'}]

        datastore_dict['fields'] = fields
        records = []
        for key, value in mapped_columns.iteritems():
            row = {'mapped_column': key,
                   'original_name': value,
                   'mapping_id': data_dict.get('resource_id')}
            records.append(row)

        datastore_dict['records'] = records
        datastore_dict['primary_key'] = 'mapped_column'

        db.create(context, datastore_dict, False)

    @staticmethod
    def create_truncated_name(original_name, truncated_columns):
        """create truncated name from original column names

        :param original_name: str, original column name
        :param truncated_columns: dict , dictionary of truncated_column name
        :return: str, dict, truncated column name, updated dictionary of truncated_column names
        """
        truncated_name = original_name[:60]

        # Sanitizing truncated name so that it can be insert into db
        truncated_name = truncated_name.replace(" ", "_").replace(",", "")

        if truncated_name in truncated_columns:
            counter = truncated_columns.get(truncated_name, 0) + 1
            truncated_columns.update({truncated_name: counter})
            truncated_name = truncated_name + '_' + str(counter)

        else:
            truncated_columns.update({truncated_name: 1})

        return truncated_name, truncated_columns

    @staticmethod
    def check_resource_list(context, data_dict):
        """Check if a resource already exists.

        :param context: context
        :param data_dict: dict, Dictionary of resource data
        :return: boolean, True or False based on the existence of the resource
        """

        package_id = data_dict.get('resource').get('package_id')
        package_data = toolkit.get_action('package_show')(
            context, {'id': package_id})
        resource_list = package_data.get('resources')
        print "Resource_size:", len(resource_list)
        # print '----------------------Package-----------------------------------------'
        # print resource_list
        # print '---------------------------------------------------------------'

        for i, resource in enumerate(resource_list):
            print "Running item", i, resource
            try:
                resource_data = toolkit.get_action('datastore_search')(
                    context, {'resource_id': resource.get('id')})
            except Exception as e:
                print e
                continue
            # print '----------------------Single resource-----------------------------------------'
            # print resource_data
            # print '---------------------------------------------------------------'
            print "................."
            print "resource_id=", data_dict.get('resource_id')
            print "package_id=", package_id
            print "................."
            records = resource_data.get('records')
            print "______________________ Record--0 ____________________________________"
            print records[0]
            print "__________________________________________________________"
            mapping_id = records[0].get('mapping_id') if records is not None else None
            print "Mapping_Id:", mapping_id
            if mapping_id is not None and mapping_id == data_dict.get('resource_id'):
                print "----------Matching--------------"
                return True
            else:
                print "Not matching..."
                continue
        print "Returning False"
        return False


