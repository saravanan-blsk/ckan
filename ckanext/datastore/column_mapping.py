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

            dataset_name = resource_data.get('name')
            data_dict.update({'resource': {}})
            data_dict['resource'].update({'name': dataset_name + '_mapping'})
            data_dict['resource'].update({'package_id': resource_data.get('package_id')})

        resource_dict = toolkit.get_action('resource_create')(
            context, data_dict['resource'])
        resource_id = resource_dict['id']
        # package_id = data_dict['resource']['package_id']

        datastore_dict['connection_url'] = data_dict['connection_url']

        datastore_dict['resource_id'] = str(resource_id)
        fields = [{'id': 'mapped_column', 'type': 'text'},
                  {'id': 'original_name', 'type': 'text'}]

        datastore_dict['fields'] = fields
        records = []
        for key, value in mapped_columns.iteritems():
            row = {'mapped_column': key, 'original_name': value}
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
