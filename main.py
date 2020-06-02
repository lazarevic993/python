import pandas as pd


def read_csv_file_to_data_frame(path):
    data_frame = pd.DataFrame(pd.read_csv(path, delimiter="|"))

    return data_frame


def make_valid_data_frame(data_frame):
    hash_data_frame = {}
    rows, col = data_frame.shape

    for i in range(0, rows):
        hash_row = {}

        for j in range(0, col-1):
            key, value = parsing_string_values(data_frame.iat[i, j])
            hash_row[key] = value
        hash_data_frame[i] = hash_row

    data_frame = (pd.DataFrame(hash_data_frame)).T
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file1.csv")

    return data_frame


def parsing_string_values(item):
    key = ""
    value = ""
    equal_sign = 0

    for char in item:

        if char != "=" and equal_sign == 0:
            key += char

        if char == "=":
            equal_sign = 1
            continue

        if char != "=" and equal_sign == 1:
            value += char

    return key, value


def remove_rows_of_column_with_specific_value(data_frame, column_name, value):
    data_frame = data_frame[getattr(data_frame, column_name) != value]
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file2.csv")

    return data_frame


def apply_upper_to_column(data_frame, column):
    data_frame[column] = data_frame[column].apply(lambda x: x.upper())
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file3.csv")

    return data_frame


def apply_service_direction_rule(data_frame):
    data_frame["ServiceDirection"] = data_frame["ServiceDirection"].apply(lambda x: "DS" if x == 1 else "US")
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file4.csv")

    return data_frame


def subs_columns_to_list(data_frame):
    data_frame = data_frame.groupby(["ServiceClassName", "CmtsHostName"]).agg({"ServicePktsPassed": "sum", "ServiceOctetsPassed": "sum", "ServiceSlaDelayPkts": "sum", "ServiceSlaDropPkts": "sum"})
    data_frame = data_frame.reset_index()
    rows, col = data_frame.shape
    subs_list = []

    file = open("/Users/Lazarevic/Projects/Ibis-Instruments/Data/list_file5.txt", "a")
    for i in range(0, rows):

        subs_list += ["put qos.ServiceOctetsPassed." + str(data_frame.CmtsHostName[i]) + " " + str(data_frame.ServiceOctetsPassed[i]) + " cmts=" + str(data_frame.CmtsHostName[i]) + "class=" + str(data_frame.ServiceClassName[i]), \
               "put qos.ServicePktsPassed." + str(data_frame.CmtsHostName[i]) + " " + str(data_frame.ServicePktsPassed[i]) + " cmts=" + str(data_frame.CmtsHostName[i]) + "class=" + str(data_frame.ServiceClassName[i]), \
               "put qos.ServiceSlaDelayPassed." + str(data_frame.CmtsHostName[i]) + " " + str(data_frame.ServiceSlaDelayPkts[i]) + " cmts=" + str(data_frame.CmtsHostName[i]) + "class=" + str(data_frame.ServiceClassName[i]), \
               "put qos.ServiceSlaDropPassed." + str(data_frame.CmtsHostName[i]) + " " + str(data_frame.ServiceSlaDropPkts[i]) + " cmts=" + str(data_frame.CmtsHostName[i]) + "class=" + str(data_frame.ServiceClassName[i])]

    file.write(str(subs_list))
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file5.csv")
    file.close()

    return subs_list


def add_to_list_unique_elements(result_list, data_frame):
    data_frame = data_frame.groupby(["ServiceDirection", "CmtsHostName"]).agg({"CmMacAddr": "nunique"})
    data_frame = data_frame.reset_index()
    rows, col = data_frame.shape

    file = open("/Users/Lazarevic/Projects/Ibis-Instruments/Data/output.txt", "a")
    for i in range(0, rows):
        result_list += ["put qos.NoOfProvisionedModems." + str(data_frame.CmtsHostName[i]) + " " + str(data_frame.CmMacAddr[i]) + " cmts=" + str(data_frame.CmtsHostName[i]) + " service_direction=" + str(data_frame.ServiceDirection[i])]

    file.write(str(result_list))
    data_frame.to_csv("/Users/Lazarevic/Projects/Ibis-Instruments/Data/csv_file6.csv")
    file.close()


df = read_csv_file_to_data_frame("/Users/Lazarevic/Projects/Ibis-Instruments/Data/data.csv")

# Task number 1. Result you can see in file "../Data/csv_file1".
df = make_valid_data_frame(df)

# Task number 2. Result you can see in file "../Data/csv_file2".
df = remove_rows_of_column_with_specific_value(df, "ServiceOctetsPassed", 0)

# Task number 3. Result you can see in file "../Data/csv_file".
df = apply_upper_to_column(df, "CmMacAddr")

#Task number 4. Result you can see in file "../Data/csv_file4".
df = apply_service_direction_rule(df)

#Task number 5. Result you can see in file "../Data/csv_file5" and file "../Data/output5.txt".
list_subs = subs_columns_to_list(df)

# #Task number 6. Result you can see in file "../Data/csv_file6" and main result file is "../Data/output.txt".
add_to_list_unique_elements(list_subs, df)
