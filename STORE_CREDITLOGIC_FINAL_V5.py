import pandas as pd
import numpy as np
import re
import os


def extract_numbers_from_string(input_string):
    try:
        # Use regular expression to find all numeric sequences in the string
        numbers = re.findall(r'\d+', input_string)
        return [int(num) for num in numbers][0]

    except Exception as e:
        return 0
        print(e)
        pass


def replace_values(list1, reference_list):
    # Create a dictionary to store the indexes of values in the reference list
    index_dict = {value: index + 1 for index, value in enumerate(reference_list)}

    # Replace values in list1 based on the indexes in the reference list
    for i in range(len(list1)):
        list1[i] = index_dict.get(list1[i], list1[i])

    return list1


dir_path = r'T:/Working Folder/Aman/Self_Learning/try/input\\'

file_no = 0
# Iterate directory
for path in os.listdir(dir_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(dir_path, path)):

        print('T:/Working Folder/Aman/Self_Learning/try/input/{}'.format(path))

        data = pd.read_csv('T:/Working Folder/Aman/Self_Learning/try/input/{}'.format(path))

        # *******************************************************************************************************************************

        new_col_mapping = ['balance_id', 'history_id', 'updated_at', 'Money flow', 'balance_amount', 'additional_info',
                           'Source', 'payment_type', 'Reference Number']
        data.columns = new_col_mapping

        data = data[['balance_id', 'history_id', 'updated_at', 'Money flow', 'balance_amount', 'additional_info',
                     'Reference Number', 'Source', 'payment_type']]

        # columns_to_sort = ['balance_id' , 'history_id']
        # sorting_orders = [True , True]

        # data = data.sort_values(by=columns_to_sort , ascending=sorting_orders)

        # data.to_csv('T:/Working Folder/Aman/R&D work Python/Store_Credit/Input_FINAL_TTTTTTTTTTTTTTT.csv')

        # *******************************************************************************************************************************

        # data = pd.read_csv('T:/Working Folder/Aman/R&D work Python/Store_Credit/Input_FINAL_TTTTTTTTTTTTTTT.csv')

        # data.loc[len(data.index)] = data.loc[len(data.index) - 1]
        # data.loc[len(data.index) - 1, 'balance_id'] = data.loc[len(data.index) - 1]['balance_id'] + 1

        # *******************************************************************************************************************************

        data['Order_number'] = data['additional_info'].apply(lambda x: float(extract_numbers_from_string(x)))

        data['Reference_Rev'] = np.where(data['Reference Number'] == 'nan', data['Order_number'],
                                         data['Reference Number'])

        data['Reference Number'] = data['Reference_Rev']

        # *******************************************************************************************************************************

        data['additional_info'] = data['additional_info'].astype(str).str.lower()
        data['Source'] = data['Source'].astype(str).str.lower()

        data['Reference Number'] = data['Reference Number'].astype(str)

        data['Order_frequency'] = data.groupby('Reference Number').cumcount() + 1
        data['Order_frequency'] = data['Order_frequency'].astype(int)

        data.loc[(data['Money flow'] > 0) & (data['Source'].str.lower() == 'order') &
                 (data['Reference Number'] == 0), 'Order_frequency'] = 1

        data.loc[(data['Money flow'] > 0) & (data['Source'].str.lower() == 'order') &
                 (data['Order_frequency'] == 1), 'Source'] = 'PAYED/CANCELLED'
        # =========================================================================================================================.

        Try_Balance_ID = []
        Try_History_ID = []
        Try_Source = []
        TRY_Date = []
        Try_Money_FLow = []
        Try_Balance_Amount = []
        Try_Deducted_Amount = []
        Try_Bucket_Final = []

        Order_List = []
        Order_Breakdown_List = []
        Order_MoneyFlow = []

        # if list(data_rev.shape)[0] > 0:

        # print('data_rev is : ' , list(data_rev.shape)[0])

        for iii in data['balance_id'].unique():
            try:

                # Try_Bucket_Final = []

                print('balance ID is : ', iii)

                data_temp = data[data['balance_id'] == iii].copy()

                data_temp['Frequency'] = data_temp.groupby('Source').cumcount() + 1
                # data_temp['Frequency'] = data_temp['Source'] + data_temp['Frequency'].astype(str)

                data_temp['Source'] = data_temp['Source'].fillna('NAN')
                condition = (data_temp['Money flow'] < 0) & (~data_temp['Source'].str.contains('order'))
                data_temp['Frequency_REV'] = 0

                for index, row in data_temp.iterrows():
                    if index > 0 and condition.loc[index]:
                        data_temp.at[index, 'Frequency_REV'] = data_temp.at[index - 1, 'Frequency']

                data_temp['Frequency'] = np.where(data_temp['Frequency_REV'] == 0, data_temp['Frequency'],
                                                  data_temp['Frequency_REV'])

                data_temp['Frequency'] = data_temp['Source'] + data_temp['Frequency'].astype(str)

                data_temp['Source'] = data_temp['Frequency'] = data_temp['Frequency'].apply(
                    lambda x: x if 'order' not in x.lower() else 'Order')

                data_temp['Source'] = data_temp['Frequency']

                # *********************************************** CHANGES TODAY ***************************************************************

                data_temp['Reverse_Flag'] = np.where(
                    (data_temp['Money flow'] < 0) & (~data_temp['Source'].str.contains('order')) &
                    (data_temp['additional_info'].str.contains('rever')), 1, 0)

                condition = data_temp['Reverse_Flag'] == 1
                
                # An issue was coming with imcomatible values assiged to dataframe therefore used below line to handle it.
                data_temp = data_temp.astype('object')


                # Update values in Column1 based on the condition
                data_temp.loc[condition, 'Reverse_Flag'] = data_temp.loc[condition, 'Reference Number']

                for value in data_temp['Reverse_Flag']:
                    index_in_column2 = data_temp.index[data_temp['Reference Number'] == value].tolist()

                    if index_in_column2:
                        index_in_column2 = index_in_column2[0]
                        value_from_column3 = data_temp.at[index_in_column2, 'Source']
                        data_temp.loc[data_temp['Reverse_Flag'] == value, 'Reverse_Flag'] = value_from_column3

                data_temp['Source'] = np.where(data_temp['Reverse_Flag'] == 0, data_temp['Source'],
                                               data_temp['Reverse_Flag'])

                data_temp.drop(['Frequency', 'Frequency_REV', 'Reverse_Flag'], axis=1, inplace=True)

                # *********************************************** CHANGES TODAY ***************************************************************

                Source = list(data_temp['Source'].unique())
                if 'Order' in Source:
                    Source.remove('Order')

                Try_Bucket = ['34'] * len(Source)

                MinVar = 1
                MaxVar = 99999

                Priority = ['34'] * len(Source)
                Balance = [0] * len(Source)

                Amount_DB_CR = 0

                list1a = []
                list1b = []

                MIN_LIST = []

                Count_index = 1
                for i, ii, order, history_ID, Date in zip(data_temp['Money flow'].to_list(),
                                                          data_temp['Source'].to_list(),
                                                          data_temp['Reference Number'].to_list(),
                                                          data_temp['history_id'].to_list(),
                                                          data_temp['updated_at']):
                    # print('\n')
                    # print('MoneyFlow values is ; ' , i)

                    a = len(str(i).split('.')[-1])
                    Balance = [round(x, a) for x in Balance]

                    if i > 0 and ii in Source:

                        MIN_LIST.append(MinVar)
                        reference_list = sorted(list(set(MIN_LIST)))
                        result_list = replace_values(MIN_LIST, reference_list)

                        Priority[Source.index(ii)] = MinVar
                        Balance[Source.index(ii)] = i

                        # /********************************************************/
                        Try_Bucket[Source.index(ii)] = result_list[-1]
                        Try_Balance_ID.append(iii)
                        Try_History_ID.append(history_ID)
                        TRY_Date.append(Date)
                        Try_Money_FLow.append(i)
                        Try_Source.append(Source[Source.index(ii)])
                        Try_Balance_Amount.append(sum(Balance))
                        Try_Deducted_Amount.append(i)
                        Try_Bucket_Final.append(Try_Bucket[Source.index(ii)])
                        # /********************************************************/

                        MinVar = MinVar + 1

                    elif i < 0:

                        i_temp = i
                        Amount_DB_CR = i
                        list1a = []

                        for i in sorted([x for x in Priority if isinstance(x, (int, float))]):

                            if Amount_DB_CR == 0:
                                break
                            else:
                                min_element = i

                                if Balance[Priority.index(min_element)] > abs(Amount_DB_CR):

                                    # /********************************************************/
                                    Try_Money_FLow.append(i_temp)
                                    Try_Balance_ID.append(iii)
                                    Try_History_ID.append(history_ID)
                                    TRY_Date.append(Date)
                                    Try_Source.append(Source[Priority.index(min_element)])
                                    Try_Deducted_Amount.append(Amount_DB_CR)
                                    Try_Bucket_Final.append(Try_Bucket[Priority.index(min_element)])
                                    # /********************************************************/

                                    list1a.append(
                                        '{}_({})'.format(Source[Priority.index(min_element)], float(Amount_DB_CR)))

                                    Balance[Priority.index(min_element)] = Balance[Priority.index(
                                        min_element)] + Amount_DB_CR
                                    Amount_DB_CR = 0

                                    Try_Balance_Amount.append(sum(Balance))

                                else:

                                    Amount_DB_CR = Amount_DB_CR + Balance[Priority.index(min_element)]

                                    list1a.append('{}_({})'.format(Source[Priority.index(min_element)],
                                                                   float(Balance[Priority.index(min_element)] * -1)))

                                    # /********************************************************/
                                    Try_Money_FLow.append(i_temp)
                                    Try_Balance_ID.append(iii)
                                    Try_History_ID.append(history_ID)
                                    TRY_Date.append(Date)
                                    Try_Source.append(Source[Priority.index(min_element)])
                                    Try_Deducted_Amount.append(-1 * Balance[Priority.index(min_element)])
                                    Try_Bucket_Final.append(Try_Bucket[Priority.index(min_element)])
                                    # /********************************************************/

                                    Balance[Priority.index(min_element)] = 0
                                    Priority[Priority.index(min_element)] = MaxVar
                                    MaxVar = MaxVar - 1

                                    Try_Balance_Amount.append(sum(Balance))

                        Order_List.append(str(order))
                        Order_Breakdown_List.append(list1a)
                        Order_MoneyFlow.append(i_temp)


                    elif i > 0 and ii not in Source:

                        i__temp = i
                        Amount_DB_CR = i

                        total = round(sum(data_temp[(data_temp['history_id'] < history_ID) & (
                                    data_temp['Source'].str.lower() == 'order') & (
                                                                data_temp['Reference Number'] == order)][
                                              'Money flow'].to_list()), len(str(i).split('.')[-1]))

                        # print('i value is ' , i)
                        # print('total values is : ' , total)
                        # print('history_ID is : ' , history_ID)
                        # print('filtered_data is : ' ,  filtered_data['Money flow'])
                        # print('\n')
                        # print('Order_List is : ' , Order_List)
                        # print('Order_Breakdown_List is : ' , Order_Breakdown_List)
                        if i <= abs(total):
                            # print('satisfy')
                            Source_Breakdown_List = [Order_Breakdown_List[index] for index, element in
                                                     enumerate(Order_List) if element == str(order)]

                            Source_Breakdown_List = [item for sublist in Source_Breakdown_List for item in sublist]
                            Source_Breakdown_List = list({}.fromkeys(Source_Breakdown_List))

                            # print(history_ID)
                            # print(Source_Breakdown_List)

                            for Source_Breakdown in Source_Breakdown_List:

                                # print('Order NUmber- {} ,  Breakdown- {} '.format(order, Source_Breakdown ) )

                                if Amount_DB_CR == 0:
                                    break
                                else:
                                    if Amount_DB_CR <= abs(float(Source_Breakdown.split('_')[-1][1:-1])):

                                        Balance[Source.index(Source_Breakdown.split('_')[0])] = Balance[Source.index(
                                            Source_Breakdown.split('_')[0])] + Amount_DB_CR

                                        Priority[Source.index(Source_Breakdown.split('_')[0])] = MinVar
                                        MinVar = MinVar + 1

                                        # /********************************************************/
                                        Try_Balance_ID.append(iii)
                                        Try_History_ID.append(history_ID)
                                        TRY_Date.append(Date)
                                        Try_Money_FLow.append(i__temp)
                                        Try_Source.append(Source_Breakdown.split('_')[0])
                                        Try_Balance_Amount.append(sum(Balance))
                                        Try_Deducted_Amount.append(Amount_DB_CR)
                                        Try_Bucket_Final.append(
                                            Try_Bucket[Source.index(Source_Breakdown.split('_')[0])])
                                        # /********************************************************/

                                        Amount_DB_CR = 0

                                    else:
                                        Amount_DB_CR = Amount_DB_CR + float(Source_Breakdown.split('_')[-1][1:-1])
                                        Balance[Source.index(Source_Breakdown.split('_')[0])] = Balance[Source.index(
                                            Source_Breakdown.split('_')[0])] + abs(
                                            float(Source_Breakdown.split('_')[-1][1:-1]))

                                        Priority[Source.index(Source_Breakdown.split('_')[0])] = MinVar
                                        MinVar = MinVar + 1

                                        # /********************************************************/
                                        Try_Balance_ID.append(iii)
                                        Try_History_ID.append(history_ID)
                                        TRY_Date.append(Date)
                                        Try_Money_FLow.append(i__temp)
                                        Try_Source.append(Source_Breakdown.split('_')[0])
                                        Try_Balance_Amount.append(sum(Balance))
                                        Try_Deducted_Amount.append(abs(float(Source_Breakdown.split('_')[-1][1:-1])))
                                        Try_Bucket_Final.append(
                                            Try_Bucket[Source.index(Source_Breakdown.split('_')[0])])




                        else:
                            # print('condition Satisfies')

                            MIN_LIST.append(MinVar)
                            reference_list = sorted(list(set(MIN_LIST)))
                            result_list = replace_values(MIN_LIST, reference_list)
                            
                            Source.append('OtherSource{}'.format(Count_index))
                            Priority.append(MinVar)
                            Try_Bucket.append(result_list[-1])

                            # print('Balance before update is ' , sum(Balance))
                            Balance.append((i))
                            # print('Balance after update is : ' , sum(Balance))
                            # /********************************************************/
                            Try_Balance_ID.append(iii)
                            Try_History_ID.append(history_ID)
                            TRY_Date.append(Date)
                            Try_Money_FLow.append(i__temp)
                            Try_Source.append('OtherSource{}'.format(Count_index))
                            Try_Balance_Amount.append(sum(Balance))
                            Try_Deducted_Amount.append(abs(i))
                            Try_Bucket_Final.append(result_list[-1])
                            # /********************************************************/

                            MinVar = MinVar + 1

                            Count_index = Count_index + 1


            except Exception as e:
                print(e)
                pass

        print(len(Try_Balance_ID))
        print(len(Try_History_ID))
        print(len(TRY_Date))
        print(len(Try_Money_FLow))
        print(len(Try_Source))

        print(len(Try_Deducted_Amount))
        print(len(Try_Balance_Amount))
        print(len(Try_Bucket_Final))

        dic = {'Balance_ID': Try_Balance_ID, 'History_ID': Try_History_ID, 'Date': TRY_Date,
               'Money_Flow': Try_Money_FLow,
               'Source': Try_Source, 'Source_Seq_No': Try_Bucket_Final, 'Transaction_Amount': Try_Deducted_Amount,
               'Balance': Try_Balance_Amount}

        datadata = pd.DataFrame(dic)

        datadata.to_csv('T:/Working Folder/Aman/Self_Learning/try/result______________{}.csv'.format(file_no))

        file_no = file_no + 1

        print(
            '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print(
            '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print(
            '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')

dir_path = r'T:/Working Folder/Aman/Self_Learning/try\\'

# list to store files
res = []

# Iterate directory
for path in os.listdir(dir_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(dir_path, path)):
        # data = pd.read_excel('T:/Working Folder/Aman/R&D work Python/Price_Comprison/Results/Latest_Result/{}'.format(path))
        print('T:/Working Folder/Aman/Self_Learning/try/{}'.format(path))
        data = pd.read_csv('T:/Working Folder/Aman/Self_Learning/try/{}'.format(path))

        res.append(data)
    print('Next File should come')

result = pd.concat(res)

# result.to_excel('T:/Working Folder/Aman/R&D work Python/Price_Comprison/Results/Latest_Result/MY_OUTPUT.xlsx')

result.to_csv('T:/Working Folder/Aman/Self_Learning/try/MY_OUTPUT.csv', index=False)




