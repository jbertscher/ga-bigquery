import pandas as pd

class GaBigQuery(object):
    def __init__(self, private_key, project_id):
        """
        :param string private_key: Private API key for BigQuery project
        :param string project_id: BigQuery project id
        """
        self.private_key = private_key
        self.project_id = project_id


    def read_views(self, query, views, start_date, end_date, view_diff, dialect = 'legacy'):
        """
        Loads data from one or more Google Analytics views in Google Big Query

        :param string query: SQL-Like Query to return data values. The following placeholders should be present:
          # {0}: will be replaced by 'view id' in each tuple passed in `views`
          # {1}: will be replaced by `start_date`
          # {2}: will be replaced by `end_date`
        :param list(tuple(view reference, view id)) views: Views to be queried, in the format `(view reference, view id)`
        :param date start_date: Start date of query (yyyy-mm-dd)
        :param date end_date: End date of query (yyyy-mm-dd)
        :param string view_diff: Header for the column that will contain `view reference` passed as part of `view`
        :param string dialect: Dialect of SQL query ('standard' / 'legacy')
        :return: DataFrame
        """
        if dialect == 'standard':
            start_date = start_date.strftime('%Y%m%d')
            end_date = end_date.strftime('%Y%m%d')
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


    def read_app_and_web_views(self, start_date, end_date, dialect = 'legacy', col_order = None,
                               app_views = None, app_query = None, web_views = None, web_query = None):
        """
        Like `read_views` but allows querying app and web views separately and combines results

        :param date start_date: Start date of query (yyyy-mm-dd)
        :param date end_date: End date of query (yyyy-mm-dd)
        :param string dialect: Dialect of SQL query ('standard' / 'legacy')
        :param col_order: Order of columns of resulting DataFrame
        :param list of tuples app_views: App views to be queried, in the format `(view reference, view id)`
        :param string app_query: SQL-Like Query to return app data values. The following placeholders should be present:
          # {0}: will be replaced by 'view id' in each tuple passed in `views`
          # {1}: will be replaced by `start_date`
          # {2}: will be replaced by `end_date`
        :param list of tuples web_views: Web views to be queried, in the format `(view reference, view id)`
        param string web_query: SQL-Like Query to return web data values. The following placeholders should be present:
          # {0}: will be replaced by 'view id' in each tuple passed in `views`
          # {1}: will be replaced by `start_date`
          # {2}: will be replaced by `end_date`
        :return:
        """

        app_defined = app_views and app_query
        web_defined = web_views and web_query

        if not app_defined and not web_defined:
            raise TypeError('Either app or web queries and views must be defined')

        if app_defined:
            app_data = self.read_views(app_query, app_views, start_date, end_date, 'device_type', dialect)
            app_data['region'] = app_data['region'].str.replace('South Africa', 'ZA').str.replace('Nigeria', 'NG')
        if web_defined:
            web_data = self.read_views(web_query, web_views, start_date, end_date, 'region', dialect)
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

