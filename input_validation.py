
def input_validation(source_query,target_query,expected_result):
    flag = False
    comment = ''
    if (source_query == '' or target_query == '') and expected_result == '':
        comment = comment + "Expected result can not be null"
    elif (source_query == '' or target_query == '') and expected_result == '':
        comment = comment = "Expected result should be null"
    if comment != '':
        flag = True
    return flag, comment


