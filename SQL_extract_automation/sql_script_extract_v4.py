import re
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

script = "hybrid_2_asylum_support_case_events"
# script = "hybrid_4_admin_review_far"

with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\Hybrid Guides\\Asylum Support"
          " Scripts\\hybrid_asylum_support-master-sql_template\\sql_template\\" + script + ".sql") as f:
    data = f.read()

table = script

"""#################################################### FUNCTIONS ###################################################"""


def remove_comments(string: str) -> str:
    return re.sub(r"(\s+)?--(?s).*?\n", "", string)


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

    try:
        string = remove_comments(string)
    except AttributeError:
        pass

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


def extract_business_rules_list(string: str) -> list:
    string = select_from_section(string)

    try:
        string = remove_comments(string)
    except AttributeError:
        pass

    string = re.sub(r"SELECT|SELECT DISTINCT|^\sDISTINCT|^DISTINCT", "", string)

    business_rules_cols = re.split(r"\s+,", string)
    business_rules_cols = [re.sub(r"\t{1,10}(\n)?|(\n$){1,10}|^(\s+)?(\n){1,10}", "", col)
                           for col in business_rules_cols]

    return business_rules_cols


def find_attribute_schema_for_table(from_anchor, table):
    non_alias_regex = r"((\[)?\w+(])?\.){1,3}" + table + r"(\s)?(\t)?(\n)"

    alias_regex = r"((\[)?\w+(])?\.(\[)?\w+(])?)(\.(\[)?\w+(])?)?\s+AS\s+" + table + r"(\s)?(\t)?(\n)" + \
                  r"|((\[)?\w+(])?\.(\[)?\w+(])?)(\.(\[)?\w+(])?)?\s+as\s+" + table + r"(\s)?(\t)?(\n)"

    reverse_from_anchor = from_anchor[::-1]
    reverse_where_groupby_pattern = table[
                                    ::-1] + r"(\s)?(SA)?\s+\)\s+\n(?s).*?(YB PUORG\s+\n.*?)?EREHW\s+(\n)(" \
                                            r"\w+\sSA\s+)?(\w+\.)?(\w+\.\w+)(\s+)?(\n)?MORF "
    spurious_where_expansion = table[::-1] + r"(\s)?(SA)?\s+\)\s+\n(\s+)?\w+\s+SA\sDNE"

    alias_schema = re.search(alias_regex, from_anchor)
    non_alias_schema = re.search(non_alias_regex, from_anchor)
    from_where_schema = re.search(reverse_where_groupby_pattern, reverse_from_anchor)
    spurious_where_expansion_schema = re.search(spurious_where_expansion, reverse_from_anchor)

    attribute_schema = ''

    if alias_schema:
        alias_schema = alias_schema.group(0)
        alias_schema = re.sub(r"\sAS\s(\w+)?(\.\w+)?|\sas\s(\w+)?(\.\w+)?", "", alias_schema)
        attribute_schema = alias_schema
    elif non_alias_schema:
        attribute_schema = non_alias_schema.group(0)
    elif from_where_schema and not spurious_where_expansion_schema:
        from_where_schema = from_where_schema.group(0)[::-1]
        from_where_schema = re.sub(r'WHERE(?s).*', "", from_where_schema)
        from_where_schema = re.sub(r'FROM|\sAS(?s).*|\s', "", from_where_schema)
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
    from_anchor = re.search(r"(?<=\nFROM)(?s).*?(?=UPDATE)?(?=;)?(?s).*(;)?", data).group(0)
    # handing sub-query
    attribute_schemas = []

    for tab in range(0, len(storing_initial_tables)):
        sub_query_table_regex = r"(?<=SELECT)(?s).*?FROM(?s).*?AS\s" + storing_initial_tables[tab] + r"(\s)?(\t)?(\n)"
        sub_query_table = re.search(sub_query_table_regex, from_anchor)
        col = columns[tab]

        try:
            non_nested_tables = re.search(r"(?<=\nFROM)(?s).*?(?<=SELECT)", data).group(0)
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

    attribute_schemas = [re.sub(r"\n{1,10}|\s+", "", col) for col in attribute_schemas]
    output = []
    for i in range(0, len(columns)):
        output.append(attribute_schemas[i] + "." + columns[i])

    return initial_attributes, output


class BusinessRules:
    def __init__(self, rule):
        self.business_rule = rule

    def replace_case_when(self):
        business_rule = re.sub(r"CASE", "", self.business_rule)
        business_rule = re.sub(r"WHEN", " When", business_rule)
        return business_rule

    def replace_then(self):
        return re.sub(r"THEN", ", then set the value to ", self.business_rule)

    def remove_end(self):
        return re.sub(r"END AS \w+|\s+END(\s+\w+)?", "", self.business_rule)

    def replace_math_symbols(self):
        business_rule = re.sub(r"\s=\s", " is equal to ", self.business_rule)
        business_rule = re.sub(r"\s>\s", " is greater than ", business_rule)
        business_rule = re.sub(r"\s<\s", " is less than ", business_rule)
        business_rule = re.sub(r"<=", "is less or equal to", business_rule)
        business_rule = re.sub(r">=", "is greater than or equal to", business_rule)
        business_rule = re.sub(r"<>|!=", "is not equal to", business_rule)

        return business_rule

    def replace_getdate(self):
        getdate_transformed = re.sub(r"GETDATE\(\)", "today", self.business_rule)
        getdate_transformed = re.sub(r"today - 1", "yesterday", getdate_transformed)
        return getdate_transformed

    def replace_datediff(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_datediff = re.search(r"DATEDIFF\(.*?\)", self.business_rule).group(0)
                transformed_datediff = re.sub(r"DATEDIFF\(DAY,", "Days from", original_datediff)
                transformed_datediff = re.sub(r",", " to", transformed_datediff)
                transformed_datediff = re.sub(r"\)", "", transformed_datediff)
                output_datediff = self.business_rule.replace(original_datediff, transformed_datediff)
                self.business_rule = output_datediff
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule

    def replace_dateadd(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_dateadd = re.search(r"DATEADD\(.*?\)", self.business_rule).group(0)
                days_to_add = re.search(r",\s\d+,", original_dateadd).group(0)

                day_match = re.search(r"\(DAY", original_dateadd) # day match
                year_match = re.search(r"\(YEAR", original_dateadd) # year match
                week_match = re.search(r"\(WEEK", original_dateadd)  # week match
                month_match = re.search(r"\(MONTH", original_dateadd)  # month match
                if day_match:
                    transformed_dateadd = re.sub(r"DATEADD\(DAY", "", original_dateadd)
                    transformed_dateadd = re.sub(r"\)", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r",\s\d+,", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r"$", r" plus" + days_to_add + r" days", transformed_dateadd)
                    transformed_dateadd = re.sub(r",", "", transformed_dateadd)
                    output_dateadd = self.business_rule.replace(original_dateadd, transformed_dateadd)
                    self.business_rule = output_dateadd
                elif year_match:
                    transformed_dateadd = re.sub(r"DATEADD\(YEAR", "", original_dateadd)
                    transformed_dateadd = re.sub(r"\)", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r",\s\d+,", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r"$", r" plus" + days_to_add + r" years", transformed_dateadd)
                    transformed_dateadd = re.sub(r",", "", transformed_dateadd)
                    output_dateadd = self.business_rule.replace(original_dateadd, transformed_dateadd)
                    self.business_rule = output_dateadd
                elif week_match:
                    transformed_dateadd = re.sub(r"DATEADD\(WEEK", "", original_dateadd)
                    transformed_dateadd = re.sub(r"\)", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r",\s\d+,", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r"$", r" plus" + days_to_add + r" weeks", transformed_dateadd)
                    transformed_dateadd = re.sub(r",", "", transformed_dateadd)
                    output_dateadd = self.business_rule.replace(original_dateadd, transformed_dateadd)
                    self.business_rule = output_dateadd
                elif month_match:
                    transformed_dateadd = re.sub(r"DATEADD\(MONTH", "", original_dateadd)
                    transformed_dateadd = re.sub(r"\)", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r",\s\d+,", "", transformed_dateadd)
                    transformed_dateadd = re.sub(r"$", r" plus" + days_to_add + r" months", transformed_dateadd)
                    transformed_dateadd = re.sub(r",", "", transformed_dateadd)
                    output_dateadd = self.business_rule.replace(original_dateadd, transformed_dateadd)
                    self.business_rule = output_dateadd
            except AttributeError:
                pass
                keep_search = 0
                pass

        return self.business_rule

    def replace_coalesce(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_coalesce = re.search(r"COALESCE\(.*?,.*?\)", self.business_rule).group(0)
                transformed_coalesce = re.sub(r"COALESCE", "Set the value to ", original_coalesce)
                transformed_coalesce = re.sub(r",", ", however if the value is not populated, set the value to ",
                                              transformed_coalesce)
                transformed_coalesce = re.sub(r"\)", "", transformed_coalesce)
                output_coaslesce = self.business_rule.replace(original_coalesce, transformed_coalesce)
                self.business_rule = output_coaslesce
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule

    def replace_cast(self):
        return re.sub(r"CAST", "", self.business_rule)

    def replace_null(self):
        null_search = re.sub(r"ELSE\s+NULL|otherwise\s+NULL", "otherwise leave it unpopulated", self.business_rule)
        null_search = re.sub(r"IS NULL", "is not populated", null_search)
        null_search = re.sub(r"IS NOT NULL", "is populated", null_search)

        return null_search

    def replace_else(self):
        return re.sub(r"ELSE", ", otherwise", self.business_rule)

    def replace_alias(self):
        return re.sub(r"AS\s+(\[)?\w+(])?", "", self.business_rule)

    def replace_in(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_in = re.search(r"IN\s\(.*?\)", self.business_rule).group(0)
                transformed_in = re.sub(r"IN\s", "is one of ", original_in)
                self.business_rule = transformed_in
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule

    def replace_count(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_count = re.search(r"COUNT\(.*?\)", self.business_rule).group(0)
                transformed_count = re.sub(r"COUNT\(", "count", original_count)
                transformed_count = re.sub(r"\)", "", transformed_count)
                self.business_rule = self.business_rule.replace(original_count, transformed_count)
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule

    def replace_over_partition(self):
        self.business_rule = re.sub(r"OVER", "", self.business_rule)
        self.business_rule = re.sub(r"PARTITION BY", "broken down by", self.business_rule)

        return self.business_rule

    def replace_max_min(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_max = re.search(r"MAX\((?s).*?\)", self.business_rule).group(0)
                transformed_max = re.sub(r"MAX\(", "take the largest value from ", original_max)
                transformed_max = re.sub(r"\)", "", transformed_max)
                self.business_rule = self.business_rule.replace(original_max, transformed_max)
            except AttributeError:
                pass
            try:
                original_min = re.search(r"MIN\((?s).*?\)", self.business_rule).group(0)
                transformed_min = re.sub(r"MIN\(", "take the smallest value from ", original_min)
                transformed_min = re.sub(r"\)", "", transformed_min)
                self.business_rule = self.business_rule.replace(original_min, transformed_min)
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule

    def replace_replace(self):
        keep_search = 1
        while keep_search == 1:
            try:
                original_replace = re.search(r"REPLACE\((?s).*?\)", self.business_rule).group(0)
                main_replace_block = re.search(r"REPLACE\(.*?,", original_replace).group(0)
                transformed_replace = re.sub(r"REPLACE\(.*?,", "", original_replace)
                transformed_replace = re.sub(r"\)", "", transformed_replace)
                transformed_replace = re.sub(r",", r" in "+main_replace_block + r" with ", transformed_replace)
                transformed_replace = re.sub(r"^", "replace ", transformed_replace)
                transformed_replace = re.sub(r"REPLACE\(", "", transformed_replace)
                self.business_rule = self.business_rule.replace(original_replace, transformed_replace)
            except AttributeError:
                keep_search = 0
                pass

        return self.business_rule


def apply_business_rule_transformer(business_rule):
    tr1 = BusinessRules(business_rule).replace_cast()
    tr2 = BusinessRules(tr1).replace_math_symbols()
    tr3 = BusinessRules(tr2).replace_getdate()
    tr4 = BusinessRules(tr3).replace_datediff()
    tr5 = BusinessRules(tr4).replace_dateadd()
    tr6 = BusinessRules(tr5).replace_coalesce()
    tr7 = BusinessRules(tr6).replace_case_when()
    tr8 = BusinessRules(tr7).replace_then()
    tr9 = BusinessRules(tr8).remove_end()
    tr10 = BusinessRules(tr9).replace_else()
    tr11 = BusinessRules(tr10).replace_null()
    tr12 = BusinessRules(tr11).replace_alias()
    tr13 = BusinessRules(tr12).replace_in()
    tr14 = BusinessRules(tr13).replace_count()
    tr15 = BusinessRules(tr14).replace_over_partition()
    tr16 = BusinessRules(tr15).replace_max_min()
    tr17 = BusinessRules(tr16).replace_replace()
    output = re.sub(r"^\s+", "", tr17)
    output = re.sub(r"\n\s+", "\n", output)
    output = re.sub(r"AND", " and", output)
    return output


def business_rule_list_transformer(string: str) -> list:
    business_rule_list = extract_business_rules_list(string)
    business_rule_list_output = [apply_business_rule_transformer(rule) for rule in business_rule_list]
    return business_rule_list_output


def extract_hybrid_columns(string: str) -> list:
    hybrid_match = re.search(r"(?<=INSERT INTO)\s+(\[)?hybrid.*(\(TABLOCK\))?(?s).*?\)(\n){1,3}?", data,
                             flags=re.DOTALL)
    cid_match = re.match(r"(?<=INSERT INTO)\s+(\[)?atomic.*(\(TABLOCK\))?(?s).*?\)(\n){1,3}?", data, flags=re.DOTALL)

    try:
        string = remove_comments(data)
    except AttributeError:
        pass

    columns = ''
    if hybrid_match or cid_match:
        match_cols = re.search(r"INSERT INTO.*(\(TABLOCK\))?(?s).*?\n\)\n", string).group(0)
        rmv_table_header = re.sub(r".*(s?).*?(\n)?\(", "", match_cols)
        rmv_tablock = re.sub(r"TABLOCK", "", rmv_table_header)
        cleaned_columns = re.sub(r"(\n){1,100}|(\t){1,100}|\s|\)|\[|]", "", rmv_tablock)
        columns = cleaned_columns.split(',')

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
columns_business_rules = extract_business_rules_list(data)
translated_business_rules = business_rule_list_transformer(data)
atlas_business_attributes = extract_business_attribute(data)

df = pd.DataFrame(
    data=[table_data, column_data, columns_business_rules, translated_business_rules, hybrid_layer_variable_data])
df = df.transpose()
df.columns = ['Table Name', 'Column Name', 'Business Rules', 'Translated Business Rules', 'Hybrid Layer Variable']

attr = pd.DataFrame(data=[atlas_business_attributes[0], atlas_business_attributes[1]])
attr = attr.transpose()
attr.columns = ['Aliased table.column', 'Business Attribute']

df.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home "
                      "Office\\Hybrid Guides\\Asylum Support Scripts\\Automated Outputs\\" + table + ".csv")
attr.to_csv(path_or_buf="C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home "
                        "Office\\Hybrid Guides\\Asylum Support Scripts\\Automated Outputs\\Attributes_" + table + ".csv")
