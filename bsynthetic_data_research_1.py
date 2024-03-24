
from random import randint
from numpy.random import uniform
from create_synthetic_data import synthetic_data_c
from os import listdir
from utils.utils import (add_path, 
                         create_folder, 
                         generate_random_key, 
                         complex_writing, 
                         series_writing, 
                         complex_reading,
                         series_reading,
                         detect_path)
from utils.request_price import get_prices_yf
from pandas import DataFrame, to_datetime

folder_main = 'synthetic_data'
folder_serie = 'serie'
folder_metric = 'metric'
file_global_infos = 'global_infos.txt'

def create_synthetic_data_series_metrics(folder_ticker,
                                         last_price,
                                         number_of_test_for_different_metric=100,
                                         number_of_tests_for_the_same_metric=300,
                                         number_of_days=2500,
                                         range_static_numbers=[-100, 100],
                                         range_index_dynamic_numbers=[1, 10],
                                         range_dynamic_number=[-100, 100]):
    """
    Gera numeros aleatorios para as variaveis da funcao que gera os dados sinteticos

    ### Args:
    >>> number_of_test_for_different_metric: numero de metricas diferentes que sera gerada para gerar os dados sinteticos
    >>> number_of_tests_for_the_same_metric: numero de series que sera gerado a partir de cada metrica diferente
    """
    folder_number_of_days = str(number_of_days)
    
    create_folder(add_path([folder_main]))
    create_folder(add_path([folder_main, folder_ticker]))
    create_folder(add_path([folder_main, folder_ticker, folder_number_of_days]))
    create_folder(add_path([folder_main, folder_ticker, folder_number_of_days, folder_serie]))
    create_folder(add_path([folder_main, folder_ticker, folder_number_of_days, folder_metric]))

    for i in range(number_of_test_for_different_metric):

        file_id = len(listdir(add_path([folder_main, folder_ticker, folder_number_of_days, folder_metric]))) + 1

        if not i < file_id:

            # define de forma aleatoria os numeros estaticos da funcao 
            static_number_M = uniform(range_static_numbers[0], range_static_numbers[1])
            static_number_S = uniform(range_static_numbers[0], range_static_numbers[1])
                
            # define o numero de elementos float aleatorios que serao aplicados na funcao de forma aleatoria
            number_of_dynamic_numbers = randint(range_index_dynamic_numbers[0], range_index_dynamic_numbers[1])
            dynamic_number_A = [uniform(range_dynamic_number[0], range_dynamic_number[1]) for _ in range(number_of_dynamic_numbers)]
            
            file_name_metric = f'{file_id}.txt'

            mensage = f'{static_number_M}, {static_number_S}, {number_of_dynamic_numbers}, {dynamic_number_A}'
            complex_writing(add_path([folder_main, folder_ticker, folder_number_of_days, folder_metric, file_name_metric]), mensage)
            
            for j in range(number_of_tests_for_the_same_metric):
                
                df_price = synthetic_data_c(last_price, None, number_of_days, static_number_M,static_number_S, dynamic_number_A)
                
                file_name_serie = f'{file_id}_{generate_random_key(32)}.txt'
                series_writing(df_price, add_path([folder_main, folder_ticker, folder_number_of_days, folder_serie, file_name_serie]))

            print('\n'*20, static_number_M, static_number_S, number_of_dynamic_numbers, dynamic_number_A, '\n')

            if input('CLOSE: ') == 'True':
                break
            
def organize_series_and_metrics(folder_ticker,
                                number_of_days):
    """
    Tabula os numeros aleatorios associando ao nome do arquivo gerado das series
    """
    list_metric_file = []
    for file in listdir(add_path([folder_main, folder_ticker, number_of_days, folder_metric])):
        metric_file = complex_reading(add_path([folder_main, folder_ticker, number_of_days, folder_metric, file]), 'read')
        list_metric_file.append([int(file.replace('.txt', ''))] + metric_file.replace('[', '').replace(']', '').split(', '))
    
    columns = ['file', 'static_number_M', 'static_number_S', 'number_of_dynamic_numbers',
               'dynamic_number_A_0', 'dynamic_number_A_1', 'dynamic_number_A_2', 'dynamic_number_A_3', 
               'dynamic_number_A_4', 'dynamic_number_A_5', 'dynamic_number_A_6', 'dynamic_number_A_7', 
               'dynamic_number_A_8', 'dynamic_number_A_9']
    
    df_data_metrics = DataFrame(list_metric_file, columns=columns).sort_values(by='file')
    df_data_metrics = df_data_metrics.fillna('nan').set_index('file')
    
    return df_data_metrics

def apply_calculation_series(folder_ticker,
                             number_of_days,
                             calculation_applied_to_the_series_1,
                             calculation_applied_to_the_series_2=None,
                             calculation_applied_to_the_series_3=None,
                             calc_name_archive='str.txt'):
    """
    Aplica calculo a todas as series geradas
    """
    path_file_calc_name = add_path([folder_main, folder_ticker, number_of_days, calc_name_archive])

    if detect_path(path_file_calc_name):
        archive = complex_reading(path_file_calc_name, 'read')
        archive = archive[1:-3].split('], [')
        list_archive = [string.split(', ') for string in archive]
        df_result_calc_series = DataFrame(list_archive)
        df_result_calc_series[0] = df_result_calc_series[0].apply(lambda x: x.replace("'", ''))
        df_result_calc_series[1] = df_result_calc_series[1].astype(float)
        df_result_calc_series[2] = df_result_calc_series[2].astype(float)
        df_result_calc_series[3] = df_result_calc_series[3].astype(float)
        return df_result_calc_series

    list_result_calc_series = []
    archives = listdir(add_path([folder_main, folder_ticker, number_of_days, folder_serie]))
    for i, file in enumerate(archives):
        id_file = file.split('_')[0]

        df_serie = series_reading(add_path([folder_main, folder_ticker, number_of_days, folder_serie, file]))
        df_serie = DataFrame(df_serie, columns=['Date', 'serie'])
        df_serie['Date'] = to_datetime(df_serie['Date'])
        df_serie = df_serie.set_index('Date').astype(float).pct_change(1).dropna()
        
        list_result = []
        result_calc_1 = df_serie.apply(calculation_applied_to_the_series_1)[0]
        list_result += [result_calc_1]
        if calculation_applied_to_the_series_2 != None:
            result_calc_2 = df_serie.apply(calculation_applied_to_the_series_2)[0]
            list_result += [result_calc_2]
        if calculation_applied_to_the_series_3 != None:
            result_calc3 = df_serie.apply(calculation_applied_to_the_series_3)[0]
            list_result += [result_calc3]
        list_result = [id_file] + list_result
        list_result_calc_series.append(list_result)
        complex_writing(add_path([folder_main, folder_ticker, number_of_days, calc_name_archive]),
                        message=f'{list_result}, ')

        print(f'{i} / {len(archives)} | {list_result}')
    
    df_result_calc_series = DataFrame(list_result_calc_series)

    return df_result_calc_series
    
if __name__  == '__main__':
    
    # -------------------------------------------------------------------------------- #
    CREATE_SYNTHETIC_DATA_SERIES_METRICS = False # False
    ORGANIZE_SERIES_AND_METRICS = False # True
    APPLY_CALCULATION_SERIES = True # True
    APPLY_CALCULATION_RESULT_SERIES = False

    ticker:str = 'goau4.sa'
    number_of_days:int = 1500
    number_of_test_for_different_metric = 100
    number_of_tests_for_the_same_metric = 300 

    # Mudar o nome quando mudar o calculo, e salvar os calculos aplicados
    calc_name_archive = 'calc_name_test_1.txt'
    calculation_applied_to_the_series_1 = lambda x: x.mean()  
    calculation_applied_to_the_series_2 = lambda x: x.std() # None
    calculation_applied_to_the_series_3 = lambda x: x.median() # None
    # -------------------------------------------------------------------------------- #

    folder_ticker = ticker.split('.')[0].upper()
    
    if CREATE_SYNTHETIC_DATA_SERIES_METRICS:
        df_price = get_prices_yf(ticker,  period='10y', column='Adj Close')
        create_synthetic_data_series_metrics(folder_ticker, 
                                             df_price.iloc[-1][0], 
                                             number_of_days=str(number_of_days),
                                             number_of_test_for_different_metric=number_of_test_for_different_metric, 
                                             number_of_tests_for_the_same_metric=number_of_tests_for_the_same_metric)

    if ORGANIZE_SERIES_AND_METRICS:
        df_data_metrics = organize_series_and_metrics(folder_ticker, 
                                                      str(number_of_days))
    
    if APPLY_CALCULATION_SERIES:
        df_result_calc_series = apply_calculation_series(folder_ticker, 
                                                         str(number_of_days),
                                                         calculation_applied_to_the_series_1,
                                                         calculation_applied_to_the_series_2,
                                                         calculation_applied_to_the_series_3,
                                                         calc_name_archive)
        print(df_result_calc_series[0])

    if APPLY_CALCULATION_RESULT_SERIES:
        pass