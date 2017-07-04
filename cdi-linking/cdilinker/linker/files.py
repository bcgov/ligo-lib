class LinkFiles:
    """
    Define filename constants used internally during linking process.
    These files are generated during liking and gets removed after that.
    """

    # File name of the left dataset clone (To make a copy and not touch the original file)
    LEFT_FILE = 'left_file.csv'
    # File name of the right dataset clone
    RIGHT_FILE = 'right_file.csv'

    MATCHED_RECORDS = 'matched_records.csv'

    # De-Duplication files
    TEMP_MATCHED_FILE = 'matched_temp.csv'
    TEMP_DEDUP_STEP_SELECTED = 'step_selected_rows.csv'
    TEMP_DEDUP_ALL_SELECTED = 'selected_rows.csv'
    TEMP_MATCHED_ENTITY_FILE = 'entity_file.csv'
    TEMP_ENTITIES_FILE = 'entities.csv'
    TEMP_STEP_REMAINED = 'remained_rows.csv'

    # Linking Files
    TEMP_LINK_FILE = 'temp_link_file.csv'
    TEMP_LINK_FILTERED = 'temp_link_filtered.csv'
    TEMP_FILTER_RECORDS = 'filtered_records.csv'
    TEMP_STEP_LINKED_FILE = 'step_linked_records.csv'
    TEMP_LINK_SORTED = 'temp_link_sorted.csv'
    TEMP_LINKING_DATA = 'temp_linking_data.csv'
    TEMP_LINKED_RECORDS = 'linked_records.csv'
    TEMP_SORTED_FILE = 'temp_sorted_data.csv'
