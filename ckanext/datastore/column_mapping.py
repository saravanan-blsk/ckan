import ckan.plugins.toolkit as toolkit
import db

_TIMEOUT = 60000  # milliseconds


class ColumnNameMapping:
    """Create data store table for mapping lengthy column names."""

    mapped_column = {}
    column_type = {}

    @staticmethod
    def create_mapping_table(context, data_dict):
        """Create mapping table for all columns in the csv resource.

        :param context: context
        :param data_dict: dict, dictionary of data
        """
        fields = data_dict.get('fields')
        resource_id = data_dict.get('resource_id')
        resource = data_dict.get('resource')
        if resource is None:
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        package_id = resource.get('package_id')
        tmp_name = resource.get('name')
        resource_name = tmp_name if tmp_name is not None else resource.get('description')
        if resource_name is None:
            resource_name_mapping = 'unknown'
        else:
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
                'mapped_name': ColumnNameMapping.sanitize_column_name(field.get('id'), idx, resource_id),
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
        ColumnNameMapping.update_data_dict(data_dict)
        db.create(context, mapping_data_dict, False)

    @staticmethod
    def update_data_dict(data_dict, result=None):
        """Update the data_dict after column name truncation (or) type from the mapping table.

        :param data_dict: dict, dictionary of data
        :param result: dict, mapping table data
        """
        resource_id = data_dict.get('resource_id')
        if ColumnNameMapping.column_type.get(resource_id) is None:
            ColumnNameMapping.column_type.update({resource_id: {}})
        if ColumnNameMapping.mapped_column.get(resource_id) is None:
            ColumnNameMapping.mapped_column.update({resource_id: {}})

        # Updating mapped_column from mapping table
        if result is not None:
            for row in result:
                ColumnNameMapping.mapped_column[resource_id].update({
                    row['original_name']: row['mapped_name']})
                ColumnNameMapping.column_type[resource_id].update({
                    row['mapped_name']: row['column_type']
                })

        # updating fields from data_dict
        for field in data_dict.get('fields'):
            if field.get('id') in ColumnNameMapping.mapped_column[resource_id]:
                original_name = field.get('id')
                mapped_name = ColumnNameMapping.mapped_column[resource_id].get(original_name)
                field['id'] = mapped_name
                field_type = ColumnNameMapping.column_type[resource_id].get(mapped_name)
                if field_type is not None:
                    field['type'] = field_type

        # updating records from data_dict
        for record in data_dict.get('records'):
            for original_name in ColumnNameMapping.mapped_column[resource_id].keys():
                mapped_name = ColumnNameMapping.mapped_column[resource_id].get(original_name)
                record[mapped_name] = record.pop(original_name)

    @staticmethod
    def sanitize_column_name(column_name, idx, resource_id):
        """Sanitize and truncate lengthy column names from data_dict

        :param column_name: str, column name to be truncated
        :param idx: integer, enumeration index of the column name
        :param resource_id: str, id of the resource
        :return: str, truncated column name
        """
        if len(column_name) < 64:
            return column_name

        truncated_name = "%s_%d" % (column_name[:58], idx)

        # Sanitizing truncated name so that it can be inserted into db
        truncated_name = truncated_name.replace(" ", "_").replace(",", "")

        # Adding entries to mapped_column list
        if ColumnNameMapping.mapped_column.get(resource_id) is None:
            ColumnNameMapping.mapped_column.update({resource_id: {}})
        ColumnNameMapping.mapped_column[resource_id].update({
            column_name: truncated_name
        })

        return truncated_name

    @staticmethod
    def get_table_schema(context, data_dict):
        """Fetch and update the data_dict schema using mapping table.

        :param context:dict, context
        :param data_dict:dict, dictionary of data
        """
        fields = data_dict.get('fields')
        if fields is None:
            raise Exception('Missing fields')

        timeout = context.get('query_timeout', _TIMEOUT)

        resource_id = data_dict.get('resource_id')
        resource_id_mapping = resource_id + '_mapping'
        try:
            # check if table already exists
            context['connection'].execute(
                u'SET LOCAL statement_timeout TO {0}'.format(timeout))
            result = context['connection'].execute(
                u'SELECT * FROM pg_tables WHERE tablename = %s',
                resource_id_mapping)

            # Check if mapping_table has records
            if not result or result.rowcount < 1:
                ColumnNameMapping.create_mapping_table(context, data_dict)
            else:
                ColumnNameMapping.update_mapping_table(context, data_dict, fields)
        except Exception, e:
            raise e

    @staticmethod
    def update_mapping_table(context, data_dict, fields):
        """
        Update mapping table and data_dict
        :param context: dict, context
        :param data_dict: dict, Data dictionary
        :param fields:
        :return:
        """
        resource_id = data_dict.get('resource_id')
        resource_id_mapping = resource_id + '_mapping'
        query = 'SELECT * FROM "%s"' % resource_id_mapping
        result = context['connection'].execute(query)
        # Check whether the schema of data_dict and mapping_table are same
        if not ColumnNameMapping.check_mapping_table_schema(fields, result):
            # Dropping mapping table so that we can insert new data.
            delete_query = 'DROP TABLE "%s"' % resource_id_mapping
            context['connection'].execute(delete_query)
            if resource_id in ColumnNameMapping.mapped_column:
                ColumnNameMapping.mapped_column.pop(resource_id)
            if resource_id in ColumnNameMapping.column_type:
                ColumnNameMapping.column_type.pop(resource_id)
            ColumnNameMapping.create_mapping_table(context, data_dict)
        else:
            result = context['connection'].execute(query)
            ColumnNameMapping.update_data_dict(data_dict, result)

    @staticmethod
    def check_mapping_table_schema(fields, result_dict):
        """
        Verify if the resource schema matches mapping_table schema.
        :param fields: dict, dictionary containing data_dict fields
        :param result_dict: dict, mapping table dictionary
        :return: bool, True (or) False
        """
        field_names = [x.get('id') for x in fields]
        mapping_table_fields = [x['original_name'] for x in result_dict]

        if len(field_names) != len(mapping_table_fields):
            return False

        if len(set(field_names).intersection(mapping_table_fields)) != len(field_names):
            return False

        return True

    @staticmethod
    def delete_mapping_table(context, resource_id):
        """
        Delete the mapping table associated with a resource id.
        :param context: dict, Context
        :param resource_id: str, Id of the resource
        """
        mapping_table_id = resource_id + '_mapping'
        select_query = 'SELECT * FROM "%s"' % mapping_table_id
        result = context['connection'].execute(select_query)
        res_exists = result.rowcount > 0

        if res_exists:
            delete_query = 'DROP TABLE "%s"' % mapping_table_id
            context['connection'].execute(delete_query)
