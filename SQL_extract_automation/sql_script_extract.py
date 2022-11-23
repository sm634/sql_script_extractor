import re
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

script = "hybrid_3_challenge_paps"

with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Challenge "
          "Scripts\\" + script + ".sql") as f:
    data = f.read()

table = script

"""Pseudo-coding requirements: 
1. Extracting columns.
2. Using the extract column function along with [database].[table] scheme to get the Hybrid Layer Variable.
3. Using similar method but modified for further search to get Business Attribute."""


def remove_comments(string: str) -> str:
    return re.sub(r"--(?s).*?(?=\n)", "", string)


def table_name(string: str, n_iteration) -> str:
    name = ''
    first = string[string.index("_") + 1:]
    for i in range(0, n_iteration):
        idx = first[1:].index("_")
        first = first[idx + 1:]
        name = first[idx + 1:]

    output = name.capitalize().replace("_", " ")
    return output


# extract_columns function: Columns to be extracted for Business Attributes. Will not maintain ordered mapping with
# columns, table and hybrid layer variable. Hence, it will be printed off separately with the correct format when
# combined with the extract_atlas_business_attribute function.
def extract_attribute_table_columns(string: str) -> list:
    data_extract = re.search(r"(?<=SELECT DISTINCT)(?s).*?,(?s).*?(\n){1,3}(?=FROM)|"
                             r"(?<=SELECT)(?s).*?(?s).*?(\n){1,3}(?=FROM)", string, flags=re.DOTALL).group(0)
    # NEED TO HANDLE: 1. comments, 2.case when columns
    try:
        data_extract = remove_comments(data_extract)  # remove comments
    except AttributeError:
        pass
    # data_extract_special_remove = re.sub(r"(\n){1,100}|(\t){1,100}", "", data_extract)  # getting rid of
    # # special characters like new
    data_extract_special_remove = re.sub(r"SELECT|DISTINCT|\sAS\s\w+|\sas\s\w+", "", data_extract)
    code_list = data_extract_special_remove.split(',')  # getting the data separated by ',' making CASE WHEN logic
    # easier to handle.
    table_columns_1 = []
    for entry in code_list:
        table_col_schema_match = re.findall(r"(\w+\.\w+)", entry)
        table_columns_1.append(table_col_schema_match)

    table_columns = []
    for item in table_columns_1:
        if len(item) == 0:
            del item
        elif len(item) == 1:
            table_columns.append(item[0])
        else:
            for j in item:
                table_columns.append(j)
    # line, tab and Aliases for column.
    return table_columns


def find_attribute_schema_for_table(from_anchor, table):
    non_alias_regex = r"((\[)?\w+(])?\.){1,3}" + table
    alias_regex = r"((\[)?\w+(])?\.(\[)?\w+(])?){1,3}\sAS\s|((\[)?\w+(])?\.(\[)?\w+(])?){1,3}\sas\s" + \
                  table
    alias_where_regex = r"(?<=FROM)(?s).*?(?=WHERE).*?AS\s" + table \
                        + "(?=\sON)?|(?<=FROM)(?s).*?(?=WHERE)(?s).*?\)\s" + table + "(?=\sON)?"
    # case where the
    # table exists as an alias, but where the actual non-aliased table is behind a where condition.
    alias_no_as_regex = r"((\[)?\w+(])?\.(\[)?\w+(])?\.(\[)?\w+(])?\s\w+)(ON)?\n|(\[)?\w+(])?\.(\[)?\w+(" \
                        r"])?\s\w+(ON)?\n" + table

    alias_schema = re.search(alias_regex, from_anchor)
    non_alias_schema = re.search(non_alias_regex, from_anchor)
    alias_where_schema = re.search(alias_where_regex, from_anchor)
    alias_no_as_schema = re.search(alias_no_as_regex, from_anchor)

    attribute_schema = ''

    if non_alias_schema:
        attribute_schema = non_alias_schema.group(0)
    elif alias_schema:
        alias_schema = alias_schema.group(0)
        alias_schema = re.sub(r"\sAS\s(\w+)?(\.\w+)?|\sas\s(\w+)?(\.\w+)?", "", alias_schema)
        attribute_schema = alias_schema
    elif alias_no_as_schema:
        alias_no_as = re.sub(r"\s\w+(ON)?\n", "", alias_no_as_schema.group(0))
        attribute_schema = alias_no_as
    elif alias_where_schema:
        alias_where_schema = re.sub(r"WHERE(?s).*|\s", "", alias_where_schema.group(0))
        alias_where_schema = re.sub(r"\sAS\s\w+|\sas\s\w+", "", alias_where_schema)
        attribute_schema = alias_where_schema

    return attribute_schema


def max_nested_from_tabs(from_anchor):
    max_tabs = r""
    max_reached = False
    temp_tabs = r"\t"
    while not max_reached:
        regex = temp_tabs + r"FROM|" + temp_tabs + r"from"
        match = re.search(regex, from_anchor)
        if match:
            max_tabs = temp_tabs
            temp_tabs += r"\t"
        else:
            max_reached = True
    return max_tabs


def extract_atlas_business_attribute(string: str) -> tuple:
    initial_attributes = extract_attribute_table_columns(string)

    columns = [col[col.index(".") + 1:] for col in initial_attributes]
    storing_initial_tables = [col[:col.index(".")] for col in initial_attributes]

    # find the table in the code after the 'FROM' command. Take the table and find its overall attribute
    # schema. This can either exist as an alias or not. So need to handle both case.
    try:
        string = remove_comments(string)  # remove comments
    except AttributeError:
        pass
    # need to handle sub-queries 'FROM'. A good thing to note is that they appear after 'tabs'. So a 'sub-from' logic
    # needs to be created.
    from_anchor = re.search(r"(?<=FROM|from)\s?\n(?s).*?(?=;)", string).group(0)
    max_from_tabs = max_nested_from_tabs(from_anchor)
    total_chars = len(max_from_tabs)

    attribute_schemas = []

    if total_chars == 0:

        for table in range(0, len(storing_initial_tables)):
            attribute_schemas.append(find_attribute_schema_for_table(from_anchor, storing_initial_tables[table]))

    else:
        tabs_from = max_from_tabs

        for i in range(0, int(total_chars / 2)):
            for table in range(0, len(storing_initial_tables)):
                nested_from_regex = tabs_from + r"FROM(?s).*(?=\))"
                nested_from_anchor = re.search(nested_from_regex, string)
                if nested_from_anchor:
                    nested_from_anchor = nested_from_anchor.group(0)
                    attribute_schemas.append(
                        find_attribute_schema_for_table(nested_from_anchor, storing_initial_tables[table]))
            if not total_chars < 2:
                tabs_from = tabs_from[:-2]
            else:
                attribute_schemas.append(find_attribute_schema_for_table(from_anchor, string))

    output = []
    for i in range(0, len(columns)):
        output.append(attribute_schemas[i] + "." + columns[i])

    return initial_attributes, output


def extract_hybrid_columns(string: str) -> list:
    hybrid_match = re.search(r"(?<=INSERT INTO\s)(\[)?hybrid", string, flags=re.DOTALL)

    try:
        string = remove_comments(string)
    except AttributeError:
        pass

    if hybrid_match:
        columns = re.search(r"INSERT INTO(?s).*?\)\n", string).group(0)
    columns = re.sub(r".*?\(", "", columns)
    columns = re.sub(r"(\n){1,100}|(\t){1,100}|\s|\)", "", columns)
    columns = columns.split(',')
    return columns


def column_names_entry(string: str) -> list:
    columns = extract_hybrid_columns(string)
    return [col.replace("_", " ") for col in columns]


def hybrid_layer_variable(string: str) -> list:
    extract_hybrid_layer_table = re.search(r"(?<=CREATE TABLE\s).*?(?=\()", string)
    hybrid_layer_table = extract_hybrid_layer_table.group(0).replace(" ", "")
    columns = extract_hybrid_columns(string)
    hybrid_table_col_join = [hybrid_layer_table + "." + col for col in columns]
    return hybrid_table_col_join


"""Writing data to csv output"""
column_data = column_names_entry(data)
table_data = [table_name(table, 1) for i in range(0, len(column_data))]
hybrid_layer_variable_data = hybrid_layer_variable(data)
atlas_business_attributes = extract_atlas_business_attribute(data)

df = pd.DataFrame(data=[table_data, column_data, hybrid_layer_variable_data])
df = df.transpose()
df.columns = ['Table Name', 'Column Name', 'Hybrid Layer Variable']

attr = pd.DataFrame(data=[atlas_business_attributes[0], atlas_business_attributes[1]])
attr = attr.transpose()
attr.columns = ['Aliased table.column', 'Business Attribute']

df.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Automated "
                      "Outputs\\" + table + ".csv")
attr.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Automated "
                        "Outputs\\attributes_" + table + ".csv")
