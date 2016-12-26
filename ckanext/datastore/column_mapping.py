import ckan.plugins.toolkit as toolkit
import db

_TIMEOUT = 60000  # milliseconds

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
    def create_mapping_table_old(context, data_dict, mapped_columns):
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
            temp_name = resource_data.get('name')
            dataset_name = temp_name if temp_name is not None else resource_data.get('description')
            data_dict.update({'resource': {}})
            data_dict['resource'].update({'name': dataset_name + '_mapping'})
            data_dict['resource'].update({'package_id': resource_data.get('package_id')})

        # Check if the resource exists
        if ColumnNameMapping.check_resource_list(context, data_dict):
            return

        resource_dict = toolkit.get_action('resource_create')(
            context, data_dict['resource'])
        resource_id = resource_dict['id']
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

        # Sanitizing truncated name so that it can be inserted into db
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

        for resource in resource_list:
            try:
                resource_data = toolkit.get_action('datastore_search')(
                    context, {'resource_id': resource.get('id')})
            except Exception:
                continue
            records = resource_data.get('records')
            mapping_id = records[0].get('mapping_id') if records is not None else None
            if mapping_id is not None and mapping_id == data_dict.get('resource_id'):
                return True

        return False

    @staticmethod
    def create_mapping_table(context, data_dict):
        """

        :param context:
        :param data_dict:
        :return:
        """
        print "ola"
        fields = data_dict.get('fields')
        resource_id = data_dict.get('resource_id')
        resource = data_dict.get('resource')
        if resource is None:
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        package_id = resource.get('package_id')
        resource_name = resource.get('name')
        resource_name_mapping = resource_name + '_mapping'
        resource_id_mapping = resource_id + '_mapping'

        mapping_fields = [
            {'id': 'mapped_name', 'type': 'text'},
            {'id': 'original_name', 'type': 'text'},
            {'id': 'column_type', 'type': 'text'},
            {'id': 'resource_id', 'type': 'text'}
        ]

        mapping_records = []
        for idx, field in enumerate(fields):
            mapping_records.append({
                'mapped_name': ColumnNameMapping.sanitize_column_name(field.get('id'), idx),
                'original_name': field.get('id'),
                'column_type': field.get('type'),
                'resource_id': resource_id
            })

        mapping_data_dict = {
            'resource_id': resource_id_mapping,
            'primary_key': 'mapped_name',
            'fields': mapping_fields,
            'records': mapping_records,
            'resource': {
                'name': resource_name_mapping,
                'package_id': package_id
            },
            'connection_url': data_dict['connection_url']
        }

        print "Feliz Navidad"
        db.create(context, mapping_data_dict, False)

    @staticmethod
    def sanitize_column_name(column_name, idx):
        if len(column_name) < 64:
            return column_name

        truncated_name = "%s_%d", (column_name[:60], idx)
        return truncated_name

    @staticmethod
    def get_table_schema(context, data_dict):
        """

        :param context:
        :param data_dict:
        :return:
        """
        fields = data_dict.get('fields')
        if fields is None:
            raise Exception('Missing fields')

        timeout = context.get('query_timeout', _TIMEOUT)

        resource_id = data_dict.get('resource_id')
        resource_id_mapping = resource_id + '_mapping'

        try:
            # check if table already existes
            context['connection'].execute(
                u'SET LOCAL statement_timeout TO {0}'.format(timeout))
            result = context['connection'].execute(
                u'SELECT * FROM pg_tables WHERE tablename = %s',
                resource_id_mapping
            ).fetchone()
            if not result:
                ColumnNameMapping.create_mapping_table(context, data_dict)
            else:
                print "Daym Daniel!!!"
        except Exception, e:
            print e
            raise e


