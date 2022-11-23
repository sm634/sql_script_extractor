Create a script which reads a sql script and extracts code lines from it.
The extraction requirements are: 
1. Columns
2. Tables
3. Hybrid Layer Variable
4. Attribute
5. Business Rules

Output: 
- The output must be a dataframe with a list of tables
with columns and business rules associated with those tables. 

Working_problems:
- The Hybrid_attribute layer is anchored under 'SELECT DISTINCT' to the end of script
including the 'FROM' command. The complication it has are: 
  1) business rules nested under the SELECT DISTINCT - FROM code section. 
  2) concatenating the columns that are SELECT DISTINCT with their corresponding tables 
  and not their aliases, which requires a more elaborate search through the texts to take
  and combine. 

Process and logic for handling attribute_layer automation. 
i) get rid of comments.
ii) place into list the sql code, separated by ',' to make dealing with individual CASE WHEN cases easier. 
iii) grab all of the table.column type entries in each list entry.
iv) ad hoc cleaning. 

Initial Solution for Attributes: 
- Just get the list of correct attributes with the right schema, this will not necessarily be
in the correct order, but it will save time from having to check for them. 


Sub_From_query: 1. Check to see if there is any FROM queries. 2. If there are 'FROM' sub-queries,
start with the most nested one. 3. Extract the relevant attribute data from there. Try the next most nested one, until you reach the initial FROM 
sql code. 4. If no sub FROM queries, then just use the initial From_anchor to extract the relevant
attributes. 


26/10/2021
- Two problems: (I) multiple FROM in sub-queries with the same number of tabs need to be picked 
up and searched for a match.
  - Potential solution: (I) get all of the 'FROM' sub-queries up until number of tabs before
  the 'FROM' sub-query -1 of ) is found. For instance, search from \t\tFrom until \t) the last one. 
(II) The table alias needs to be matched to its proper full_name schema.
  - Ans: (II) can be fixed by changing the logic for find_attribute_schemas_for_table function 
  to not return just the initial table alias name if a match is not found. 

Handling Sub-query: 
when searching in sub-query, (1) it should first look for the column name within a 'SELECT'-'FROM' statement. 
(2) If it finds that, then it should look for the column name associated with the attribute table schema.
(3) Identify any alias, if any that this column is found with.
(4) find the schema associated with the alias and find the attribute. 
(5) Importantly, this is just a recursion step of the same process already used to extract attributes. 


17/11/2021
- When picking up attributes, we need to allow for table_alias.column_name to have the brackets used to pick out the 
table name around it. e.g. [identity].handle_space (an example from hybrid_1_asylum_support_biograpics script).
