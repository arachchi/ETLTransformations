import abc


class BaseDataExporter:
    def __init__(self):
        pass

    @abc.abstractmethod
    def export_data(self, data_frame):
        pass


class CsvDataExporter(BaseDataExporter):
    """
    Export dataframe to CSV file
    """

    def __init__(self):
        super().__init__()
        self._file_name = None

    @property
    def file_name(self):
        """
        Get file name
        :return: file name
        """
        return self._file_name

    @file_name.setter
    def file_name(self, file):
        """
        Set file name
        :param file:
        :return:
        """
        self._file_name = file

    def export_data(self, data_frame):
        """
        Export data to the given csv file
        :param data_frame:
        :return:
        """
        data_frame.to_csv(self._file_name)


class PrintDataExporter(BaseDataExporter):
    """
    Print Dataframe
    """

    def __init__(self):
        super().__init__()

    def export_data(self, data_frame):
        """
        Export dataframe to console
        :param data_frame:
        :return:
        """
        print(data_frame.head())
