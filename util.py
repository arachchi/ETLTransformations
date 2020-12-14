from datetime import datetime
from urllib.parse import urlparse


def extract_url_levels(url_string: str):
    """
    Extract url levels from url string
    :param url_string:
    :return:
    """
    if url_string is None or not url_string:
        return None, None, None

    url_string = url_string.strip()
    if not url_string.startswith("https://") and not url_string.startswith("http://"):
        url_string = f"https://{url_string}"
    extracted_url = urlparse(url_string)
    path = extracted_url.path
    url_levels = path.split("/")
    for level in range(1, 4):
        if len(url_levels) < level:
            url_levels.append(None)

    hostname = extracted_url.hostname.replace("www.", "").replace("w3.", "")
    url_level_1 = hostname
    url_level_2 = url_levels[1]
    url_level_3 = url_levels[2]

    return url_level_1, url_level_2, url_level_3


def extract_datetime(date_string: str):
    """
    Convert data time format to time bucket format
    :param date_string: date in the format of %d/%m/%Y %H:%M:%S
    :return: time bucket in the format of %Y%m%d%H
    """
    if date_string is None or not date_string:
        return date_string

    datetime_input_format = "%d/%m/%Y %H:%M:%S"
    datetime_output_format = "%Y%m%d%H"

    datetime_object = datetime.strptime(date_string, datetime_input_format)

    datetime_bucket = datetime_object.strftime(datetime_output_format)
    return datetime_bucket
