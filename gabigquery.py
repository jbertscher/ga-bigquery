import pandas as pd

class GaBigQuery(object):
    def __init__(self, private_key, project_id):
        """
        :param string private_key: Private API key for BigQuery project
        :param string project_id: BigQuery project id
        """
        self.private_key = private_key
        self.project_id = project_id


    def load_device_type(self, query, views, start_date, end_date, view_diff, dialect = 'legacy'):
        """
        Creates DataFrame with specified SQL query to BigQuery and Google Analytics views

        :param string query: Query to pass to BigQuery
        :param list of tuples views: Views to be queried, in the format `(<view reference>, <view id>)`
        :param date start_date: Start date of query
        :param date end_date: End date of query
        :param string view_diff: What differentiates each view. This will be set as the header for the column
        that contains <view reference> (see explanation of `views`)
        :param string dialect: Dialect of SQL query ('standard' / 'legacy')
        :return: DataFrame
        """
        data = None
        for v in views:
            next_view = pd.read_gbq(query.format(v[1], start_date, end_date),
                                    project_id = self.project_id,
                                    private_key = self.private_key,
                                    dialect = dialect);
            next_view[view_diff] = v[0]
            if data is None:
                data = next_view
            else:
                data = pd.concat([data, next_view], axis=0)
        return data


    def load_all_views(self, start_date, end_date, dialect = 'legacy', col_order = None,
                   app_views = None, app_query = None, web_views = None, web_query = None):
        """
        Creates DataFrame with specified SQL query to BigQuery and Google Analytics views that differ by device type
        (Android vs iOS) and/or region

        :param date start_date: Start date of query
        :param date end_date: End date of query
        :param string dialect: Dialect of SQL query ('standard' / 'legacy')
        :param col_order: Order of columns of resulting DataFrame
        :param list of tuples app_views: App views to be queried, in the format `(<view reference>, <view id>)`
        :param string app_query: App query to pass to BigQuery
        :param list of tuples web_views: Web views to be queried, in the format `(<view reference>, <view id>)`
        param string web_query: Web query to pass to BigQuery
        :return:
        """

        app_defined = app_views and app_query
        web_defined = web_views and web_query

        if not app_defined and not web_defined:
            raise TypeError('Either app or web queries and views must be defined')

        if app_defined:
            app_data = self.load_device_type(app_query, app_views, start_date, end_date, 'device_type', dialect)
            app_data['region'] = app_data['region'].str.replace('South Africa', 'ZA').str.replace('Nigeria', 'NG')
        if web_defined:
            web_data = self.load_device_type(web_query, web_views, start_date, end_date, 'region', dialect)
            web_data['device_type'] = 'web'
            web_data['appVersion'] = 'NA'
        if app_defined and web_defined:
            combined_data = pd.concat([app_data, web_data], axis=0)
            if col_order is None:
                col_order = app_data.columns
            return combined_data[col_order]
        if app_defined:
            return app_data
        if web_defined:
            return web_data

