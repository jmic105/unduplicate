# unduplicate
A set of geographic functions to help remove adjacent duplicate addresses commonly found in clinical datasets.

- These functions work by looking at adjacent or row-by-row address duplicates 
- This is important when there are columns with temporal data associated with an address in patient geographic records
- Addresses can be duplicates but are not recorded in the same way (e.g., "123 Main Street" vs. "123 Main St" ) 
- These functions were developed to provide various approaches to remove row-by-row address duplicates
- They work by providing a grouping variable for the inputted dataframe
- In many cases when working with clinical data, this will be the patient record identifier

HOW TO USE THESE FUNCTIONS
the user calls the functions by providing the dataframe, at least one grouping variable, and any other needed parameters
if a parameter is required but is not provided, the function will send an error message
the returned dataframe will provide the duplicate address rows based on the function used
Note: these functions are set up to be used with at least one grouping variable to constrain the search space within the dataframe
optionally, if the parameter "in.context" is set to TRUE, the function returns both duplicate rows for the user to see them "in context"
the returned dataframe containing duplicate rows can be used with the `anti_join()` function to filter out only those rows
the "arrange.by" parameter will the sort dataframe based on user input
both "filter.blanks" and "filter.pobox" will filter out rows where address columns are either blank or contain PO Box addresses only

**Example:**  
**df_with_duplicates <- get_sim_addrs(your_df, group.vars, addr.col, filter.blanks = TRUE)**  
**your_new_df <- anti_join(your_df, df_with_duplicates)**

- get_same_coords = determines duplicate longitude/latitude
- get_same_addrs = determines exact address duplicates
- get_sim_addrs = attempts to match similar addresses by comparing the first six character strings of an address
- get_sim_text = attempts to match only the first four characters of the text string portion of an address
- get_adj_addrs = determines duplicate addresses that are on adjacent rows when there is more than one address column in the dataset
- get_same_nums = determines duplicate numeric identifiers in a set of addresses
- get_sim_street_names = attempts to match similar street labels
- get_facil_names = determines duplicate addresses by facility name
- get_same_dates = determines address duplicates based on the same date
- get_precise_text = allows the user to set the regular expression for precise string matching
