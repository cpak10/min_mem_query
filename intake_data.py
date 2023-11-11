import os
import kaggle

if __name__ == '__main__':

    if not os.path.exists('intake_data'):
        os.mkdir('intake_data')
        with open('intake_data/.gitignore', 'wt', encoding='utf-8') as file_gitignore:
            file_gitignore.write('*\n')
        print('intake_data folder created')
    else:
        print('intake_data folder already exists')

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('mattiuzc/stock-exchange-data', path='intake_data',
                                      unzip=True)
