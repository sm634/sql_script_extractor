import re
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# script1 = "hybrid_5_admin_review_pcdp_main"
# script2 = "hybrid_2_challenge_paps"
# # script2 = "hybrid_6_challenge_appeal"
#
# with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Challenge "
#           "Scripts\\" + script1 + ".sql") as f:
#     data = f.read()
#
# with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\PAPs\\Challenge "
#           "Scripts\\" + script2 + ".sql") as f2:
#     data2 = f2.read()

script = "hybrid_1_asylum_support_biographics_1"
# script = "hybrid_4_admin_review_far"

with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\Hybrid Guides\\Asylum Support"
          " Scripts\\hybrid_asylum_support-master-sql_template\\sql_template\\" + script + ".sql") as f:
    data = f.read()

table = script

"""############################################## Test code below ###################################################"""


def remove_comments(string: str) -> str:
    return re.sub(r"(\s+)?--(?s).*?\n", "", string)


def select_from_section(string: str) -> str:
    return re.search(r"(?<=SELECT DISTINCT)(?s).*?,(?s).*?(\n){1,3}(?=FROM)|"
                     r"(?<=SELECT)(?s).*?(?s).*?(\n){1,3}(?=FROM)", string, flags=re.DOTALL).group(0)


def find_attribute_schema_for_table(from_anchor, table):
    non_alias_regex = r"((\[)?\w+(])?\.){1,3}" + table + r"(\s)?(\t)?(\n)"

    alias_regex = r"((\[)?\w+(])?\.(\[)?\w+(])?)(\.(\[)?\w+(])?)?\s+AS\s+" + table + r"(\s)?(\t)?(\n)" + \
                  r"|((\[)?\w+(])?\.(\[)?\w+(])?)(\.(\[)?\w+(])?)?\s+as\s+" + table + r"(\s)?(\t)?(\n)"

    reverse_from_anchor = from_anchor[::-1]
    reverse_where_groupby_pattern = table[
                                    ::-1] + r"(\s)?(SA)?\s+\)\s+\n(?s).*?(YB PUORG\s+\n.*?)?EREHW\s+(\n)(\w+\sSA\s+)?(\w+\.)?(\w+\.\w+)(\s+)?(\n)?MORF"
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


from_anchor = re.search(r"(?<=\nFROM)(?s).*?(?=UPDATE)?(?=;)?(?s).*(;)?", data).group(0)
tab = "biographics"


non_nested_tables = re.search(r"(?<=\nFROM)(?s).*?(?<=SELECT)", data).group(0)
test_exist = re.search(tab, non_nested_tables).group(0)  # handling cases where there are
# sub-queries but tables that are not nested in them.
print(find_attribute_schema_for_table(non_nested_tables, tab))

breakpoint()
