from data_transformer import transform_to_url_summary, transform_to_action_summary


class EtlProcessor:

    def __init__(self, data, url_summary_data_exporter, time_bucket_data_exporter):
        self.data = data
        self.data_exporter = url_summary_data_exporter
        self.time_bucket_data_exporter = time_bucket_data_exporter

    def transform_to_url_level_activities(self):
        """
        Transform data_frame to URL summary format and export
        :return:
        """
        data_frame = self.data.copy()
        transformed_data = transform_to_url_summary(data_frame)
        self.data_exporter.export_data(transformed_data)

    def transform_to_time_bucket(self):
        """
        Transform data_frame to time_bucket summary format and export
        :return:
        """
        data_frame = self.data.copy()
        transformed_data = transform_to_action_summary(data_frame)
        self.time_bucket_data_exporter.export_data(transformed_data)
