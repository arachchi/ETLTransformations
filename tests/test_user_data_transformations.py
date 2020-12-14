import unittest

from data_exporter import CsvDataExporter, PrintDataExporter
from data_loader import load_data_from_json_file, load_data_from_json_list
from etl_processor import EtlProcessor


class TestUserDataTransformations(unittest.TestCase):

    def test_loading_given_data(self):
        df = load_data_from_json_file('./assets/source_event_data.json')
        url_summary_data_exporter = CsvDataExporter()
        url_summary_data_exporter.file_name = "url_summary_data.csv"
        time_bucket_data_exporter = CsvDataExporter()
        time_bucket_data_exporter.file_name = "time_bucket_summary.csv"
        etl_processor = EtlProcessor(df, url_summary_data_exporter, time_bucket_data_exporter)

        etl_processor.transform_to_url_level_activities()
        etl_processor.transform_to_time_bucket()

    def test_transform_to_url_level_activities(self):
        df = load_data_from_json_file('./assets/source_event_data.json')
        url_summary_data_exporter = CsvDataExporter()
        url_summary_data_exporter.file_name = "url_summary_data.csv"
        time_bucket_data_exporter = CsvDataExporter()
        time_bucket_data_exporter.file_name = "time_bucket_summary.csv"
        etl_processor = EtlProcessor(df, url_summary_data_exporter, time_bucket_data_exporter)

        etl_processor.transform_to_url_level_activities()

    def test_transform_to_time_bucket(self):
        df = load_data_from_json_file('./assets/source_event_data.json')
        url_summary_data_exporter = CsvDataExporter()
        url_summary_data_exporter.file_name = "url_summary_data.csv"
        time_bucket_data_exporter = CsvDataExporter()
        time_bucket_data_exporter.file_name = "time_bucket_summary.csv"
        etl_processor = EtlProcessor(df, url_summary_data_exporter, time_bucket_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_url_summary_data(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": 56456, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564561", "id": 56456, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_url_level_activities()

    def test_time_bucket_summary_test_case_1(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": 56456, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 56456, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_time_bucket_summary_test_case_2(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": 564561, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 564562, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_time_bucket_summary_test_case_3(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 564562, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_time_bucket_summary_test_case_4(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": None,
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 564562, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_time_bucket_summary_test_case_5(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hiapages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 564562, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": None}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_time_bucket()

    def test_action_summary_test_case_1(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": "564563", "id": 564562, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_url_level_activities()

    def test_action_summary_test_case_2(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": None, "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_url_level_activities()

    def test_action_summary_test_case_3(self):
        data_list = [
            {"event_id": "893479324983546", "user": {"session_id": "564561", "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"},
            {"event_id": "893479324983546",
             "user": {"session_id": None, "id": None, "ip": "111.222.333.4"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:22:00"}
        ]
        df = load_data_from_json_list(data_list)
        print_data_exporter = PrintDataExporter()
        etl_processor = EtlProcessor(df, print_data_exporter, print_data_exporter)

        etl_processor.transform_to_url_level_activities()
