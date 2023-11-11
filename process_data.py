import os
import json


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


if __name__ == '__main__':

    if not os.path.exists('database'):
        os.mkdir('database')
        with open('database/.gitignore', 'wt', encoding='utf-8') as file_gitignore:
            file_gitignore.write('*\n')
        with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata:
            file_metadata.write('{}')
        print('database folder created')
    else:
        print('database folder already exists')

    with open('database/metadata.json', 'rt', encoding='utf-8') as file_metadata:
        dict_metadata = json.loads(file_metadata.read())

    dict_data = {'info': 'indexInfo', 'processed': 'indexProcessed'}
    if 'index_values' in dict_metadata:
        index_values = set(dict_metadata['index_values'])
    else:
        index_values = set()
    next_index = generator_index(index_values)

    for table_name, table in dict_data.items():
        if table_name in dict_metadata:
            print(f'{table_name} already found in database, please delete if updates needed')
            continue
        table_index = {'index': []}

        with open(f'intake_data/{table}.csv', 'rt', encoding='utf-8-sig') as csv_input:
            HEADER = True
            for line in csv_input.readlines():
                if HEADER:
                    table_columns = line[:-1].lower().split(',')
                    table_index['columns'] = table_columns
                    HEADER = False
                    continue
                index = next(next_index)
                index_values.add(index)
                with open(f'database/{index}.csv', 'wt', encoding='utf-8') as csv_output:
                    csv_output.write(line[:-1])
                table_index['index'].append(index)

        dict_metadata[table_name] = table_index
    dict_metadata['index_values'] = list(index_values)

    with open('database/metadata.json', 'wt', encoding='utf-8') as file_metadata:
        json_metadatastring = json.dumps(dict_metadata)
        file_metadata.write(json_metadatastring)
