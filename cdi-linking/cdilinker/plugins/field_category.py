import csv
from collections import Counter

from cdilinker.plugins.base import PluginMount
from cdilinker.config.config import config
link_config = config.get_section('LINKER')
CHUNK_SIZE = int(link_config.get('chunk_size') or '100000')


class FieldCategory(metaclass=PluginMount):
    def __init__(self):
        self.name = ''
        self.title = ''
        self.outcomes = []

    def get_counts(self, data_filename, field_name):

        data_size = 0  # Total number of rows
        data_len = 0  # Total number of rows with available data for the given field

        counts = Counter()
        with open(data_filename, 'r') as data_file:
            reader = csv.reader(data_file)
            header = next(reader)
            field_index = header.index(field_name)

            for row in reader:
                value = row[field_index]
                if value and len(value.strip()) > 0:
                    counts[value.strip()] += 1
                    data_len += 1
                data_size += 1

        counts = sorted(counts.items(), key=lambda x: x[0])
        return counts, data_size, data_len

    def get_u_values(self, left_file, right_file, left_field, right_field):

        left_counts, left_size, left_len = self.get_counts(left_file, left_field)
        left_missing = left_size - left_len
        right_counts, right_size, right_len = self.get_counts(right_file, right_field)
        right_missing = right_size - right_len

        left_itr = iter(left_counts)
        right_itr = iter(right_counts)

        u_values = Counter()
        total = 0
        try:
            next_left = next(left_itr)
            next_right = next(right_itr)
            while True:
                if next_left[0] < next_right[0]:
                    next_left = next(left_itr)
                elif next_right[0] < next_left[0]:
                    next_right = next(right_itr)
                else:
                    u_values[next_left[0]] = (next_left[1] * next_right[1]) / float(left_len * right_len)
                    total += next_left[1] * next_right[1]
                    next_left = next(left_itr)
                    next_right = next(right_itr)
        except StopIteration as err:
            pass

        u_data = dict()
        u_data['Values'] = dict(u_values.most_common(CHUNK_SIZE))
        u_data['Agreement'] = total / float(left_size * right_size)
        u_data['Disagreement'] = (left_len * right_len - total) / float(left_size * right_size)

        u_data['Missing'] = (left_missing * right_size + right_missing *
                             left_size - left_missing * right_missing) / float(left_size * right_size)

        return u_data


class FirstnameField(FieldCategory):
    name = 'firstname_field'

    def __init__(self):
        super(FirstnameField).__init__()
        self.title = 'First name field'
        self.outcomes = ['Agreement', 'Disagreement', 'Missing']


class LastnameField(FieldCategory):
    name = ''

    def __init__(self):
        super(LastnameField).__init__()
        self.name = 'lastname_field'
        self.title = 'Last name field'
        self.outcomes = ['Agreement', 'Disagreement', 'Missing']


class MiddlenameField(FieldCategory):
    name = 'middlename_field'

    def __init__(self):
        super(MiddlenameField).__init__()
        self.title = 'Middle name field'
        self.outcomes = ['Agreement', 'Disagreement', 'Missing']


class DateField(FieldCategory):
    name = 'data_field'

    def __init__(self):
        self.title = 'Date of Birth field'
        self.outcomes = ['Agreement', 'Disagreemnt', 'Missing', 'Swapped']


class SINField(FieldCategory):
    name = 'sin_field'

    def __init__(self):
        self.name = 'sin_field'
        self.title = 'SIN field'
        self.outcomes = ['Agreement', 'Disagreemnt', 'Missing']


class GeneralStringField(FieldCategory):
    name = 'string_field'

    def __init__(self):
        self.name = 'string_field'
        self.title = 'General String field'
        self.outcomes = ['Agreement', 'Disagreemnt']


class GeneralNumericField(FieldCategory):
    name = 'numeric_field'

    def __init__(self):
        self.name = 'numeric_field'
        self.title = 'General Numeric field'
        self.outcomes = ['Agreement', 'Disagreemnt']
