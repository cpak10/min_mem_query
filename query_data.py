import os
import json
import time
import pandas as pd


def view_data(metadata: dict, arguments: dict) -> None:
    '''
    Returns a view of the selected data.

    Example Command Line
    --------------------
    view(table=info, limit=20, columns=index currency, where=currency is USD)
    
    Query Options
    -------------
    table : str
        name of table
    limit : int
        default 10, number of observation to limit output, max is all
    columns : string
        column names separated by spaces (e.g. index currency)
    where : str
        where to project the table, default all rows (e.g. column is value)

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    table_index = metadata[table_name]['index']

    if 'columns' in arguments:
        table_columns_unformatted = arguments['columns']
        table_columns = set(table_columns_unformatted.split(' '))

    else:
        table_columns = set(metadata[table_name]['columns'])

    if 'limit' in arguments:
        if arguments['limit'] == 'max':
            table_limit = len(table_index)
        else:
            table_limit = int(arguments['limit'])
    else:
        table_limit = 10
    table_index_narrowed = table_index[:table_limit]

    if 'where' in arguments:
        column_to_check, value_to_check = arguments['where'].split(' is ')
        position = metadata[table_name]['columns'].index(column_to_check)

    array_table = []
    for index in table_index_narrowed:
        with open(f'database/{index}.csv', 'rt', encoding='utf-8') as file_csv:
            line_full = file_csv.read().split(',')
            if 'where' in arguments:
                if str(line_full[position]).lower() == value_to_check:
                    array_table.append(
                        [line_full[position] for position, column in enumerate(
                            metadata[table_name]['columns']) if column in table_columns])
            else:
                array_table.append(
                    [line_full[position] for position, column in enumerate(
                        metadata[table_name]['columns']) if column in table_columns])
    df_table = pd.DataFrame(array_table, columns=[column for position, column in enumerate(
        metadata[table_name]['columns']) if column in table_columns])
    print(df_table)


def delete_data(metadata: dict, arguments: dict) -> None:
    '''
    Deletes selected data from database.

    Example Command Line
    --------------------
    delete(table=info, where=currency is USD)
    
    Query Options
    -------------
    table : str
        name of table
    where : str
        where to delete the table, default all rows (e.g. column is value)

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    table_index = metadata[table_name]['index']
    columns = metadata[table_name]['columns']
    index_values = set(metadata['index_values'])
    if 'where' in arguments:
        column_to_check, value_to_check = arguments['where'].split(' is ')
        position = columns.index(column_to_check)
        list_index_scrap = []
        for index in table_index:
            with open(f'database/{index}.csv', 'rt', encoding='utf-8') as file_csv:
                line_full = file_csv.read().split(',')
                if str(line_full[position]).lower() == value_to_check.strip():
                    list_index_scrap.append(index)
        for index in list_index_scrap:
            os.remove(f'database/{index}.csv')
            table_index.remove(index)
            index_values.remove(index)
        metadata[table_name]['index'] = table_index
        metadata['index_values'] = list(index_values)
    else:
        for index in table_index:
            os.remove(f'database/{index}.csv')
            index_values.remove(index)
        del metadata[table_name]
        metadata['index_values'] = list(index_values)
    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def sort_data(metadata: dict, arguments: dict) -> None:
    '''
    Sorts data in selected table by selected column

    Example Command Line
    --------------------
    sort(table=info, on=currency, invert=n)
    
    Query Options
    -------------
    table : str
        name of table
    on : str
        column to sort on
    invert : str
        y/n for inverting the sort

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    table_index = metadata[table_name]['index']
    columns = metadata[table_name]['columns']
    on_column = arguments['on']
    position = columns.index(on_column)
    if 'invert' in arguments:
        sort_invert = arguments['invert']
    else:
        sort_invert = 'n'

    if ('sort' in metadata[table_name] and metadata[table_name]['sort'] == on_column and
        metadata[table_name]['sort_invert'] == sort_invert):
        print(f'already sorted on {on_column}')
        return

    if sort_invert == 'y':
        sorted_list = helper_merge_sort(table_index, position, True)
    else:
        sorted_list = helper_merge_sort(table_index, position, False)

    metadata[table_name]['sort'] = on_column
    metadata[table_name]['sort_invert'] = sort_invert
    metadata[table_name]['index'] = sorted_list

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def helper_merge_sort(list_index: list, position: int, reverse: bool=False) -> list:
    '''
    Apply recursive merge sort

    Parameters
    ----------
    list_index : list
        list of index values
    position : int
        position of column to check
    reverse : bool
        whether to reverse the sort order
    
    Returns
    -------
    sorted_list : list
        sorted list of index values
    '''
    if len(list_index) == 1:
        return list_index
    if len(list_index) <= 2:
        with open(f'database/{list_index[0]}.csv', 'rt', encoding='utf-8') as file_csv:
            first_value = file_csv.read().split(',')[position]
        with open(f'database/{list_index[1]}.csv', 'rt', encoding='utf-8') as file_csv:
            second_value = file_csv.read().split(',')[position]
        if first_value <= second_value:
            if reverse:
                return [list_index[1], list_index[0]]
            return list_index
        if reverse:
            return list_index
        return [list_index[1], list_index[0]]

    list_first_half = list_index[:len(list_index)//2]
    list_second_half = list_index[len(list_index)//2:]
    list_first_sorted = helper_merge_sort(list_first_half, position, reverse)
    list_second_sorted = helper_merge_sort(list_second_half, position, reverse)
    first_pointer = 0
    second_pointer = 0
    sorted_list = []
    with open(f'database/{list_first_sorted[first_pointer]}.csv', 'rt',
                encoding='utf-8') as file_csv:
        first_value = file_csv.read().split(',')[position]
    with open(f'database/{list_second_sorted[second_pointer]}.csv', 'rt',
                encoding='utf-8') as file_csv:
        second_value = file_csv.read().split(',')[position]
    while first_pointer < len(list_first_sorted) or second_pointer < len(list_second_sorted):
        if first_pointer == len(list_first_sorted):
            sorted_list.extend(list_second_sorted[second_pointer:])
            second_pointer = len(list_second_sorted)
        elif second_pointer == len(list_second_sorted):
            sorted_list.extend(list_first_sorted[first_pointer:])
            first_pointer = len(list_first_sorted)
        else:
            if reverse:
                if first_value >= second_value:
                    sorted_list.append(list_first_sorted[first_pointer])
                    first_pointer += 1
                    if first_pointer < len(list_first_sorted):
                        with open(f'database/{list_first_sorted[first_pointer]}.csv', 'rt',
                                encoding='utf-8') as file_csv:
                            first_value = file_csv.read().split(',')[position]
                else:
                    sorted_list.append(list_second_sorted[second_pointer])
                    second_pointer += 1
                    if second_pointer < len(list_second_sorted):
                        with open(f'database/{list_second_sorted[second_pointer]}.csv', 'rt',
                                encoding='utf-8') as file_csv:
                            second_value = file_csv.read().split(',')[position]
            else:
                if first_value <= second_value:
                    sorted_list.append(list_first_sorted[first_pointer])
                    first_pointer += 1
                    if first_pointer < len(list_first_sorted):
                        with open(f'database/{list_first_sorted[first_pointer]}.csv', 'rt',
                                encoding='utf-8') as file_csv:
                            first_value = file_csv.read().split(',')[position]
                else:
                    sorted_list.append(list_second_sorted[second_pointer])
                    second_pointer += 1
                    if second_pointer < len(list_second_sorted):
                        with open(f'database/{list_second_sorted[second_pointer]}.csv', 'rt',
                                encoding='utf-8') as file_csv:
                            second_value = file_csv.read().split(',')[position]
    return sorted_list


def create_data(metadata: dict, arguments: dict) -> None:
    '''
    Creates a new table in database

    Example Command Line
    --------------------
    create(table=info_new, columns = index usd)
    
    Query Options
    -------------
    table : str
        name of table
    columns : str
        string of columns to make new table

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    string_columns = arguments['columns'].split(' ')

    if table_name in metadata:
        print(f'{table_name} already in database')
        return

    metadata[table_name] = {'index': [], 'columns': string_columns}

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def generator_index(index_set: set) -> int:
    '''
    Takes a set of integers, finds the next available integer.

    Parameters
    ----------
    index_set : set
        set of integers
    
    Returns
    -------
    index_check : int
        integer
    '''
    index_check = 0
    while True:
        if index_check in index_set:
            index_check += 1
        else:
            yield index_check
            index_check += 1


def insert_data(metadata: dict, arguments: dict) -> None:
    '''
    Inserts data in selected table by selected column

    Example Command Line
    --------------------
    insert(table=info, data= oneword two|word three|word|string)
    
    Query Options
    -------------
    table : str
        name of table
    data : str
        string of data to load in, add in spaces using pipes |

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    string_data = arguments['data']
    table_index = metadata[table_name]['index']
    columns = metadata[table_name]['columns']
    index_values = set(metadata['index_values'])

    list_data = string_data.split(' ')    
    if len(list_data) == len(columns):
        list_data_clean = [item.replace('|', ' ') for item in list_data]
        next_index = generator_index(index_values)
        index = next(next_index)
        index_values.add(index)
        with open(f'database/{index}.csv', 'wt', encoding='utf-8') as csv_file:
            csv_file.write(','.join(list_data_clean))
        table_index.append(index)
    else:
        print('unexpected number of values')
        return

    if 'sort' in metadata[table_name]:
        del metadata[table_name]['sort']
        del metadata[table_name]['sort_invert']
    metadata[table_name]['index'] = table_index
    metadata['index_values'] = list(index_values)

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def update_data(metadata: dict, arguments: dict) -> None:
    '''
    Updates data in selected table by selected column

    Example Command Line
    --------------------
    update(table=info, where=currency is USD, to=US Dollars)
    
    Query Options
    -------------
    table : str
        name of table
    where : str
        string where to place data formated with column is value
    to : str
        value to replace with

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    table_name = arguments['table']
    column_to_replace, value_to_replace = arguments['where'].split(' is ')
    table_index = metadata[table_name]['index']
    columns = metadata[table_name]['columns']
    position = columns.index(column_to_replace)
    value_to_insert = arguments['to']

    for index in table_index:
        with open(f'database/{index}.csv', 'r+t', encoding='utf-8') as csv_file:
            list_original_file = csv_file.read().split(',')
            if str(list_original_file[position]).lower() == value_to_replace:
                list_original_file[position] = value_to_insert
                csv_file.seek(0)
                csv_file.write(','.join(list_original_file))
                csv_file.truncate()

    if 'sort' in metadata[table_name] and metadata[table_name]['sort'] == column_to_replace:
        del metadata[table_name]['sort']
        del metadata[table_name]['sort_invert']

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def refresh_metadata() -> dict:
    '''
    Refresh metadata, helper function

    Parameters
    ----------
    None

    Returns
    -------
    metadata : dict
        metadata refresh
    '''
    with open('database/metadata.json', 'rt', encoding='utf-8') as file_metadata_rewrite:
        metadata = json.loads(file_metadata_rewrite.read())
    return metadata


def merge_data(metadata: dict, arguments: dict) -> None:
    '''
    Merges data on an inner join between two tables on a key

    Example Command Line
    --------------------
    merge(left=info, right=processed, left_on=index, right_on=index, name=info_processed)
    
    Query Options
    -------------
    left : str
        table name for the left table
    right : str
        table name for the right table
    left_on : str
        key on left table
    right_on : str
        key on right table
    name : str
        name of new table after merge

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    left_table = metadata[arguments['left']]
    right_table = metadata[arguments['right']]
    name = arguments['name']

    set_left_columns = set(left_table['columns'])
    right_columns = right_table['columns']
    list_right_columns_unique = [value for value in right_columns if value not in set_left_columns]
    list_right_positions = [
        right_columns.index(value) for value in list_right_columns_unique]
    list_right_nospace = [value.replace(' ', '') for value in list_right_columns_unique]
    all_columns = left_table['columns'] + list_right_nospace
    left_position = left_table['columns'].index(arguments['left_on'])
    right_position = right_table['columns'].index(arguments['right_on'])

    create_data(metadata, {'table':f'{name}', 'columns':f'{" ".join(all_columns)}'})
    metadata = refresh_metadata()

    left_index_all = metadata[arguments['left']]['index']
    right_index_all = metadata[arguments['right']]['index']
    index_values = set(metadata['index_values'])
    next_index = generator_index(index_values)
    new_index = []
    for left_index in left_index_all:
        with open(f'database/{left_index}.csv', 'rt', encoding='utf-8') as csv_file:
            list_left_values = csv_file.read().split(',')
        left_key = str(list_left_values[left_position]).lower()
        for right_index in right_index_all:
            with open(f'database/{right_index}.csv', 'rt', encoding='utf-8') as csv_file:
                list_right_values = csv_file.read().split(',')
            right_key = str(list_right_values[right_position]).lower()
            if left_key == right_key:
                index = next(next_index)
                index_values.add(index)
                list_writing = list_left_values + [
                    list_right_values[i] for i in list_right_positions]
                with open(f'database/{index}.csv', 'wt', encoding='utf-8') as csv_file:
                    csv_file.write(','.join(list_writing))
                new_index.append(index)
    metadata['index_values'] = list(index_values)
    metadata[name]['index'] = new_index

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


def group_data(metadata: dict, arguments: dict) -> None:
    '''
    Perform aggregate functions on given table

    Example Command Line
    --------------------
    group(table=processed, on=index, perform=sum, using=open, to=processed_group)
    
    Query Options
    -------------
    table : str
        table to perform aggregate functions
    on : str
        column to group on
    perform : str
        aggregate function to perform
    using : str
        column to aggregate
    to : str
        table name to save to

    Parameters
    ----------
    metadata : dict
        database metadata created at data processing
    arguments : dict
        arguments passed through the query
    
    Returns
    -------
    None
    '''
    to_name = arguments['to']
    group_column_name = arguments['on']
    aggregate_column_name = arguments['using']
    table_name = arguments['table']

    create_data(metadata, {
        'table':f'{to_name}', 'columns':f'{group_column_name} {aggregate_column_name}'})
    metadata = refresh_metadata()

    if ('sort' not in metadata[table_name] or
        metadata[table_name]['sort'] != group_column_name):
        sort_data(metadata, {'table': f'{table_name}', 'on': f'{group_column_name}'})
        metadata = refresh_metadata()

    table_index = metadata[table_name]['index']
    on_position = metadata[table_name]['columns'].index(group_column_name)
    aggregate_position = metadata[table_name]['columns'].index(aggregate_column_name)
    aggregate_function = arguments['perform']
    index_values = set(metadata['index_values'])
    next_index = generator_index(index_values)
    new_index = []

    previous_on = None
    current_value = 0
    value_count  = 0

    for index in table_index:
        with open(f'database/{index}.csv', 'rt', encoding='utf-8') as csv_file:
            list_full_line = csv_file.read().split(',')
            on_value = list_full_line[on_position]
            value = float(list_full_line[aggregate_position])
        if not previous_on:
            previous_on = on_value
            current_value += value
            value_count += 1
        elif on_value == previous_on:
            current_value += value
            value_count += 1
        else:
            index_write = next(next_index)
            index_values.add(index_write)
            with open(f'database/{index_write}.csv', 'wt', encoding='utf-8') as csv_file:
                if aggregate_function == 'sum':
                    csv_file.write(f'{previous_on},{current_value}')
                elif aggregate_function == 'avg':
                    csv_file.write(f'{previous_on},{current_value/value_count}')
            new_index.append(index_write)
            previous_on = on_value
            current_value = 0
            current_value += value
            value_count = 1
    index_write = next(next_index)
    index_values.add(index_write)
    with open(f'database/{index_write}.csv', 'wt', encoding='utf-8') as csv_file:
        if aggregate_function == 'sum':
            csv_file.write(f'{previous_on},{current_value}')
        elif aggregate_function == 'avg':
            csv_file.write(f'{previous_on},{current_value/value_count}')
    new_index.append(index_write)

    metadata['index_values'] = list(index_values)
    metadata[to_name]['index'] = new_index

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata_rewrite:
        file_metadata_rewrite.write(json.dumps(metadata))


if __name__ == '__main__':

    while True:
        query_text = input('\nenter query: ')
        start_time = time.time()
        query_text_strip = query_text.strip().lower()
        if query_text_strip == 'exit':
            print('database connection closed')
            break

        location_first_parenthesis = query_text_strip.find('(')
        operative_text = query_text_strip[location_first_parenthesis+1:]

        dict_arguments = {}
        STRING_KEY = STRING_VALUE = ''
        FLAG_KEY = True
        for letter in operative_text:
            if letter == '=':
                FLAG_KEY = False
            elif letter in (',', ')'):
                FLAG_KEY = True
                dict_arguments[STRING_KEY.strip()] = STRING_VALUE.strip()
                STRING_KEY = STRING_VALUE = ''
            else:
                if FLAG_KEY:
                    STRING_KEY += letter
                else:
                    STRING_VALUE += letter

        with open('database/metadata.json', 'rt', encoding='utf-8') as file_metadata:
            dict_metadata = json.loads(file_metadata.read())

        if query_text_strip[:location_first_parenthesis] == 'view':
            view_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'delete':
            delete_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'sort':
            sort_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'create':
            create_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'insert':
            insert_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'update':
            update_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'merge':
            merge_data(dict_metadata, dict_arguments)
        elif query_text_strip[:location_first_parenthesis] == 'group':
            group_data(dict_metadata, dict_arguments)

        end_time = time.time()
        print(f'\nelapsed time: {round(end_time-start_time, 2)} sec')
