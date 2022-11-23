import re
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

script = "hybrid_4_challenge_appeal"
# script = "hybrid_4_admin_review_far"

with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Challenge "
          "Scripts\\" + script + ".sql") as f:
    data = f.read()

table = script

"""#################################################### FUNCTIONS ###################################################"""


def remove_comments(string: str) -> str:
    return re.sub(r"--(?s).*?(?=\n)", "", string)


def select_from_section(string: str) -> str:
    return re.search(r"(?<=SELECT DISTINCT)(?s).*?,(?s).*?(\n){1,3}(?=FROM)|"
                     r"(?<=SELECT)(?s).*?(?s).*?(\n){1,3}(?=FROM)", string, flags=re.DOTALL).group(0)


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
    string = select_from_section(string)

    # data_extract_special_remove = re.sub(r"(\n){1,100}|(\t){1,100}", "", data_extract)  # getting rid of
    # # special characters like new
    data_extract_special_remove = re.sub(r"SELECT|DISTINCT|\sAS\s\w+|\sas\s\w+", "", string)
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


def extract_columns_with_business_rules(string: str) -> list:
    string = select_from_section(string)

    try:
        string = remove_comments(string)
    except AttributeError:
        pass

    string = re.sub(r"SELECT|SELECT DISTINCT|^\sDISTINCT|^DISTINCT", "", string)
    business_rules_cols = string.split('\t,')
    business_rules_cols = [re.sub(r"\n{2,10}|\t{1,10}|(\n$){1,10}|^(\n){1,10}", "", col) for col in business_rules_cols]

    return business_rules_cols


def find_attribute_schema_for_table(from_anchor, table):
    non_alias_regex = r"((\[)?\w+(])?\.){1,3}" + table + r"(\s)?(\t)?(\n)"

    alias_regex = r"((\[)?\w+(])?\.(\[)?\w+(])?){1,3}((\s){1,10}?(\t){1,10})?(\s)?AS\s" + table + \
                  r"|((\[)?\w+(])?\.(\[)?\w+(])?){1,3}((\s){1,10}?(\t){1,10})(\s)??as\s" + table

    from_where_regex = r"(?<=FROM)\s(\w+\.\w+)(\.\w+)?(\sAS\s\w+)?\n(\t){1,5}?WHERE\s.*\n(\t){1,5}\)\s(AS\s)?" + table + r"(\s)?(\t)?(\n)?"

    alias_schema = re.search(alias_regex, from_anchor)
    non_alias_schema = re.search(non_alias_regex, from_anchor)
    from_where_schema = re.search(from_where_regex, from_anchor)

    attribute_schema = ''

    if alias_schema:
        alias_schema = alias_schema.group(0)
        alias_schema = re.sub(r"\sAS\s(\w+)?(\.\w+)?|\sas\s(\w+)?(\.\w+)?", "", alias_schema)
        attribute_schema = alias_schema
    elif non_alias_schema:
        attribute_schema = non_alias_schema.group(0)
    elif from_where_schema:
        from_where_schema = from_where_schema.group(0)
        from_where_schema = re.sub(r'WHERE(?s).*', "", from_where_schema)
        from_where_schema = re.sub(r'\sAS(?s).*|\s', "", from_where_schema)
        attribute_schema = from_where_schema

    return attribute_schema


def extract_business_attribute(string: str) -> tuple:
    initial_attributes = extract_attribute_table_columns(string)
    columns = [col[col.index(".") + 1:] for col in initial_attributes]
    storing_initial_tables = [col[:col.index(".")] for col in initial_attributes]

    # find the table in the code after the 'FROM' command. Take the table and find its overall attribute
    # schema. This can either exist as an alias or not. So need to handle both case.
    try:
        string = remove_comments(string)  # remove comments
    except AttributeError:
        pass
    # Searching for table schema for the noted attributes and aliases.
    from_anchor = re.search(r"(?<=FROM|from)\s?\n(?s).*?(?=UPDATE)?(?=;)", string).group(0)
    # handing sub-query
    attribute_schemas = []

    for tab in range(0, len(storing_initial_tables)):
        sub_query_table_regex = r"(?<=SELECT)(?s).*?FROM(?s).*?AS\s" + storing_initial_tables[tab]
        sub_query_table = re.search(sub_query_table_regex, from_anchor)
        col = columns[tab]

        try:
            non_nested_tables = re.search(r"(?=FROM|from)(?s).*?SELECT", string).group(0)
            re.search(storing_initial_tables[tab], non_nested_tables).group(0)  # handling cases where there are
            # sub-queries but tables that are not nested in them.
            attribute_schemas.append(find_attribute_schema_for_table(non_nested_tables, storing_initial_tables[tab]))
        except AttributeError:
            if sub_query_table:

                table_sub_query = sub_query_table.group(0)
                # search for column with other alias.
                col_with_alias_regex = r"\w+\." + col
                col_with_alias = re.search(col_with_alias_regex, table_sub_query)
                if col_with_alias:
                    new_schema = col_with_alias.group(0)
                    table_alias = new_schema[:new_schema.index(".")]
                    # search for that alias within the sub-query
                    attribute_schemas.append(find_attribute_schema_for_table(table_sub_query, table_alias))
                else:
                    attribute_schemas.append(
                        find_attribute_schema_for_table(table_sub_query, storing_initial_tables[tab]))
            else:
                attribute_schemas.append(find_attribute_schema_for_table(from_anchor, storing_initial_tables[tab]))

    output = []
    for i in range(0, len(columns)):
        output.append(attribute_schemas[i] + "." + columns[i])

    return initial_attributes, output


def extract_hybrid_columns(string: str) -> list:
    hybrid_match = re.search(r"(?<=INSERT INTO\s)(\[)?hybrid", string, flags=re.DOTALL)
    cid_match = re.match(r"(?<=INSERT INTO\s)(\[)?atomic", string, flags=re.DOTALL)

    try:
        string = remove_comments(string)
    except AttributeError:
        pass

    columns = ''
    if hybrid_match or cid_match:
        columns = re.search(r"INSERT INTO(?s).*?\)\n", string).group(0)
    columns = re.sub(r".*?\(", "", columns)
    columns = re.sub(r"(\n){1,100}|(\t){1,100}|\s|\)", "", columns)
    columns = columns.split(',')
    return columns


def column_names_entry(string: str) -> list:
    columns = extract_hybrid_columns(string)
    return [col.capitalize().replace("_", " ") for col in columns]


def hybrid_layer_variable(string: str) -> list:
    extract_hybrid_layer_table = re.search(r"(?<=INSERT INTO\s).*?(?=\s)", string)
    hybrid_layer_table = extract_hybrid_layer_table.group(0).replace(" ", "")
    columns = extract_hybrid_columns(string)
    hybrid_table_col_join = [hybrid_layer_table + "." + col for col in columns]
    return hybrid_table_col_join


"""Writing data to csv output"""
column_data = column_names_entry(data)
table_data = [table_name(table, 1) for i in range(0, len(column_data))]
hybrid_layer_variable_data = hybrid_layer_variable(data)
columns_business_rules = extract_columns_with_business_rules(data)
atlas_business_attributes = extract_business_attribute(data)

df = pd.DataFrame(data=[table_data, column_data, columns_business_rules, hybrid_layer_variable_data])
df = df.transpose()
df.columns = ['Table Name', 'Column Name', 'Business Rules', 'Hybrid Layer Variable']

attr = pd.DataFrame(data=[atlas_business_attributes[0], atlas_business_attributes[1]])
attr = attr.transpose()
attr.columns = ['Aliased table.column', 'Business Attribute']

df.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Automated "
                      "Outputs\\" + table + ".csv")
attr.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Automated "
                        "Outputs\\attributes_" + table + ".csv")
