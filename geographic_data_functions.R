### Functions to help reduce and filter address duplicates from geographic datasets ###

## These functions work by looking at adjacent or row-by-row address duplicates -- this is important when there are columns with temporal
## data associated with an address in patient geographic records -- addresses can be duplicates but are not recorded
## in the same way (e.g., "123 Main Street" vs. "123 Main St" ) -- these functions were developed to provide various approaches
## to remove row-by-row address duplicates -- these functions work by providing a grouping variable for the inputted
## dataframe - in many cases when working with clinical data -- this will be the patient record identifier

## HOW TO USE THESE FUNCTIONS ##
## the user calls the functions by providing the dataframe, at least one grouping variable, and any other needed parameters
## if a parameter is required but is not provided, the function will send an error message
## the returned dataframe will provide the duplicate address rows based on the function used
## Note: these functions are set up to be used with at least one grouping variable to constrain the search space within the dataframe
## optionally, if the parameter "in.context" is set to TRUE, the function returns both duplicate rows for the user to see them "in context"
## the returned dataframe containing duplicate rows can be used with the `anti_join()` function to filter out only those rows
## the "arrange.by" parameter will the sort dataframe based on user input
## both "filter.blanks" and "filter.pobox" will filter out rows where address columns are either blank or contain PO Box addresses only

## e.g., df_with_duplicates <- get_sim_addrs(your_df, group.vars, addr.col, filter.blanks = TRUE)
## your_new_df <- anti_join(your_df, df_with_duplicates)

#get_same_coords = determines duplicate longitude/latitude
#get_same_addrs = determines exact address duplicates
#get_sim_addrs = attempts to match similar addresses by comparing the first six character strings of an address
#get_sim_text = attempts to match only the first four characters of the text string portion of an address
#get_adj_addrs = determines duplicate addresses that are on adjacent rows when there is more than one address column in the dataset
#get_same_nums = determines duplicate numeric identifiers in a set of addresses
#get_sim_street_names = attempts to match similar street labels
#get_facil_names = determines duplicate addresses by facility name
#get_same_dates = determines address duplicates based on the same date
#get_precise_text = allows the user to set the regular expression for precise string matching

library(tidyverse)

#function to determine longitude/latitude duplicates on subsequent rows
get_same_coords <- function(df, group.vars, lon, lat, in.context = FALSE, arrange.by) {

  if (in.context) {

    lon_lat1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LOC = paste({{lon}}, {{lat}}, sep = ', '),
             LAG_COPY = lag(LOC)) %>%
      dplyr::filter(LAG_COPY == LOC)

    lon_lat2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LOC = paste({{lon}}, {{lat}}, sep = ', '),
             LEAD_COPY = lead(LOC)) %>%
      dplyr::filter(LEAD_COPY == LOC)

    lon_lat_df <- bind_rows(lon_lat2, lon_lat1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LOC, LAG_COPY, LEAD_COPY))

  }

  else {

    lon_lat_df <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LOC = paste({{lon}}, {{lat}}, sep = ', '),
             LAG_COPY = lag(LOC)) %>%
      dplyr::filter(LAG_COPY == LOC) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LOC, LAG_COPY))

  }

  cat('Found', nrow(lon_lat_df), 'duplicate(s).')

  return(lon_lat_df)

}

#function to determine exact address duplicates (the same on subsequent rows)
get_same_addrs <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                           filter.blanks = FALSE, in.context = FALSE, arrange.by) {

  if (filter.blanks && in.context) {

    addr_df_1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_COPY = lag(paste({{addr.col}}))) %>%
      dplyr::filter(LAG_COPY == {{addr.col}})

    addr_df_2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LEAD_COPY = lead(paste({{addr.col}}))) %>%
      dplyr::filter(LEAD_COPY == {{addr.col}})

    addr_duplicates <- bind_rows(addr_df_2, addr_df_1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::filter({{addr.col}} != '') %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LAG_COPY, LEAD_COPY))

  }

  else if (filter.blanks && in.context == FALSE) {

    addr_duplicates <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_COPY = lag(paste({{addr.col}}))) %>%
      dplyr::filter(LAG_COPY == {{addr.col}}) %>%
      dplyr::filter({{addr.col}} != '') %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LAG_COPY))

  }

  else if (filter.blanks == FALSE && in.context) {

    addr_df_1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_COPY = lag(paste({{addr.col}}))) %>%
      dplyr::filter(LAG_COPY == {{addr.col}})

    addr_df_2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LEAD_COPY = lead(paste({{addr.col}}))) %>%
      dplyr::filter(LEAD_COPY == {{addr.col}})

    addr_duplicates <- bind_rows(addr_df_2, addr_df_1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LAG_COPY, LEAD_COPY))

  }

  else {

    addr_duplicates <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_COPY = lag(paste({{addr.col}}))) %>%
      dplyr::filter(LAG_COPY == {{addr.col}}) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LAG_COPY))

  }

  cat('Found', nrow(addr_duplicates), 'duplicate(s).')

  return(addr_duplicates)

}

#function to determine similarly written addresses
get_sim_addrs <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                          filter.blanks = FALSE, in.context = FALSE, arrange.by) {

  if (filter.blanks && in.context) {

    sim_df_1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Za-z|\\s]{1,6}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(LAG_COPY == ADDR_COPY) %>%
      dplyr::filter({{addr.col}} != '')


    sim_df_2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Za-z|\\s]{1,6}'),
             LEAD_COPY = lead(ADDR_COPY)) %>%
      dplyr::filter(LEAD_COPY == ADDR_COPY) %>%
      dplyr::filter({{addr.col}} != '')

    sim_addrs <- bind_rows(sim_df_2, sim_df_1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY, LEAD_COPY))

  }

  else if (filter.blanks && in.context == FALSE) {

    sim_addrs <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Za-z|\\s]{1,6}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(LAG_COPY == ADDR_COPY) %>%
      dplyr::filter({{addr.col}} != '') %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

  }

  else if (filter.blanks == FALSE && in.context == TRUE) {

    sim_df_1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Z|\\s]{1,6}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(LAG_COPY == ADDR_COPY)


    sim_df_2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Za-z|\\s]{1,6}'),
             LEAD_COPY = lead(ADDR_COPY)) %>%
      dplyr::filter(LEAD_COPY == ADDR_COPY)

    sim_addrs <- bind_rows(sim_df_2, sim_df_1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY, LEAD_COPY))

  }

  else {

    sim_addrs <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(ADDR_COPY = str_extract({{addr.col}}, '^[0-9]+[A-Za-z|\\s]{1,6}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      filter(LAG_COPY == ADDR_COPY) %>%
      ungroup() %>%
      select(-c(ADDR_COPY, LAG_COPY))

  }

  cat('Found', nrow(sim_addrs), 'duplicate(s).')

  return(sim_addrs)

}

#function to determine similar text portion of addresses
get_sim_text <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                         in.context = FALSE, arrange.by) {

  if (in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '[A-Za-z]{2,4}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(LAG_COPY == ADDR_COPY)

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '[A-Za-z]{2,4}'),
             LEAD_COPY = lead(ADDR_COPY)) %>%
      dplyr::filter(LEAD_COPY == ADDR_COPY)

    sim_text <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY, LEAD_COPY))

  }

  else {

    sim_text <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '[A-Za-z]{2,4}'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(LAG_COPY == ADDR_COPY) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

  }

  cat('Found', nrow(sim_text), 'text duplicate(s).')

  return(sim_text)

}

#function to determine address duplicates on adjacent rows between two address columns
get_adj_addrs <- function(df, group.vars, first.addr = stop('Two addresses must be provided.'),
                          second.addr = stop('Two addresses must be provided.'), filter.pobox = FALSE,
                          in.context = FALSE, arrange.by) {

  if (filter.pobox && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = lag({{first.addr}})) %>%
      dplyr::filter(ADDR_COPY == {{second.addr}}) %>%
      dplyr::filter(ADDR_COPY != '') %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', ADDR_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_2_COPY = lead({{second.addr}})) %>%
      dplyr::filter(ADDR_2_COPY == {{first.addr}}) %>%
      dplyr::filter(ADDR_2_COPY != '') %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', ADDR_2_COPY))

    adj_addrs <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, ADDR_2_COPY))


  }

  else if (filter.pobox == FALSE && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = lag({{first.addr}})) %>%
      dplyr::filter(ADDR_COPY == {{second.addr}}) %>%
      dplyr::filter(ADDR_COPY != '')

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_2_COPY = lead({{second.addr}})) %>%
      dplyr::filter(ADDR_2_COPY == {{first.addr}}) %>%
      dplyr::filter(ADDR_2_COPY != '')

    adj_addrs <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY, ADDR_2_COPY))

  }

  else if (filter.pobox && in.context == FALSE) {

    adj_addrs <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = lag({{first.addr}})) %>%
      dplyr::filter({{second.addr}} == ADDR_COPY) %>%
      #dplyr::filter({{first.addr}} != '') %>%
      dplyr::filter({{second.addr}} != '') %>%
      #dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX', {{first.addr}})) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{second.addr}})) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY))

  }

  else {

    adj_addrs <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = lag({{first.addr}})) %>%
      dplyr::filter({{second.addr}} == ADDR_COPY) %>%
      #dplyr::filter({{first.addr}} != '') %>%
      dplyr::filter({{second.addr}} != '') %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(ADDR_COPY))

  }

  cat('Found', nrow(adj_addrs), 'duplicate(s).')

  return(adj_addrs)

}

#function to determine numeric duplicates
get_same_nums <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                          filter.pobox = FALSE, in.context = FALSE, arrange.by) {

  if (filter.pobox && in.context) {

    df1 <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LAG_COPY = trimws(lag(NUMS))) %>%
      filter(NUMS == LAG_COPY) %>%
      filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      select(-c(NUMS, LAG_COPY))

    df2 <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LEAD_COPY = trimws(lead(NUMS))) %>%
      filter(NUMS == LEAD_COPY) %>%
      filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      select(-c(NUMS, LEAD_COPY))

    addr_nums <- bind_rows(df2, df1) %>%
      arrange(across({{arrange.by}})) %>%
      ungroup()

  }

  else if (filter.pobox && in.context == FALSE) {

    addr_nums <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LAG_COPY = trimws(lag(NUMS))) %>%
      filter(NUMS == LAG_COPY) %>%
      filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      select(-c(NUMS, LAG_COPY))

  }

  else if (filter.pobox == FALSE && in.context) {

    df1 <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LAG_COPY = trimws(lag(NUMS))) %>%
      filter(NUMS == LAG_COPY) %>%
      select(-c(NUMS, LAG_COPY))

    df2 <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LEAD_COPY = trimws(lead(NUMS))) %>%
      filter(NUMS == LEAD_COPY) %>%
      select(-c(NUMS, LEAD_COPY))

    addr_nums <- bind_rows(df2, df1) %>%
      arrange(across({{arrange.by}})) %>%
      ungroup()

  }


  else {

    addr_nums <- df %>%
      group_by(across({{group.vars}})) %>%
      mutate(NUMS = str_extract({{addr.col}}, '[0-9]+'),
             LAG_COPY = trimws(lag(NUMS))) %>%
      filter(NUMS == LAG_COPY) %>%
      select(-c(NUMS, LAG_COPY))

  }

  cat('Found', nrow(addr_nums), 'duplicate(s).')

  return(addr_nums)

}

#function to determine similar street labels
get_sim_street_names <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                                 filter.pobox = FALSE, in.context = FALSE, arrange.by) {

  if (filter.pobox && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LAG_COPY = lag(LABEL)) %>%
      dplyr::filter(LABEL == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(LABEL, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LEAD_COPY = lead(LABEL)) %>%
      dplyr::filter(LABEL == LEAD_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(LABEL, LEAD_COPY))

    street_names <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }

  else if (filter.pobox && in.context == FALSE) {

    street_names <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(STREET_LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LAG_COPY = lag(STREET_LABEL)) %>%
      dplyr::filter(STREET_LABEL == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(STREET_LABEL, LAG_COPY)) %>%
      dplyr::ungroup()

  }

  else if (filter.pobox == FALSE && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LAG_COPY = lag(LABEL)) %>%
      dplyr::filter(LABEL == LAG_COPY) %>%
      dplyr::select(-c(LABEL, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LEAD_COPY = lead(LABEL)) %>%
      dplyr::filter(LABEL == LEAD_COPY) %>%
      dplyr::select(-c(LABEL, LEAD_COPY))

    street_names <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }

  else {

    street_names <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(STREET_LABEL = str_extract({{addr.col}}, '\\b[A-Za-z]{5,}'),
             LAG_COPY = lag(STREET_LABEL)) %>%
      dplyr::filter(STREET_LABEL == LAG_COPY) %>%
      dplyr::select(-c(STREET_LABEL, LAG_COPY)) %>%
      dplyr::ungroup()

  }

  cat('Found', nrow(street_names), 'similar street names.')

  return(street_names)

}

#function to determine duplicate facility names
get_facil_names <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                            filter.pobox = FALSE, in.context = FALSE, arrange.by) {

  if (filter.pobox && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Za-z]{3,}(?=\\s[A-Za-z])'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Za-z]{3,}(?=\\s[A-Za-z])'),
             LEAD_COPY = lead(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LEAD_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LEAD_COPY))

    facil_names <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }


  else if (filter.pobox && in.context == FALSE) {

    facil_names <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Za-z]{3,}(?=\\s[A-Za-z])'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

  }


  else if (filter.pobox == FALSE && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Z]{3,}(?=\\s[A-Za-z])'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LAG_COPY) %>%
      #filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Z]{3,}(?=\\s[A-Za-z])'),
             LEAD_COPY = lead(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LEAD_COPY) %>%
      #filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LEAD_COPY))

    facil_names <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }

  else {

    facil_names <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(ADDR_COPY = str_extract({{addr.col}}, '^[A-Za-z]{3,}(?=\\s[A-Za-z])'),
             LAG_COPY = lag(ADDR_COPY)) %>%
      dplyr::filter(ADDR_COPY == LAG_COPY) %>%
      #filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX', {{addr.col}})) %>%
      dplyr::select(-c(ADDR_COPY, LAG_COPY))

  }

  cat('Found', nrow(facil_names), 'duplicate facility names.')

  return(facil_names)

}

#START HERE
#function to determine addresses with the same date per grouping variable in "group.vars" parameter
get_same_dates <- function(df, group.vars, date.col = stop('Date variable must be provided.'),
                           in.context = FALSE, arrange.by) {

  if (in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_DT = lag({{date.col}})) %>%
      dplyr::filter({{date.col}} == LAG_DT) %>%
      dplyr::select(-c(LAG_DT))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LEAD_DT = lead({{date.col}})) %>%
      dplyr::filter({{date.col}} == LEAD_DT) %>%
      dplyr::select(-c(LEAD_DT))

    same_dts <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()


  }

  else {

    same_dts <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LAG_DT = lag({{date.col}})) %>%
      dplyr::filter({{date.col}} == LAG_DT) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c(LAG_DT))

  }

  cat('Found', nrow(same_dts), 'dates with more than one address.')

  return(same_dts)

}

#function to adjust the precision of the regular expression that accounts for duplicate addresses with various
#formatting
get_precise_text <- function(df, group.vars, addr.col = stop('Address variable must be provided.'),
                             filter.pobox = FALSE, pattern = '', in.context = FALSE, arrange.by) {

  if (filter.pobox && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, {{pattern}}),
             LAG_COPY = lag(LABEL)) %>%
      dplyr::filter(LABEL == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(LABEL, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, {{pattern}}),
             LEAD_COPY = lead(LABEL)) %>%
      dplyr::filter(LABEL == LEAD_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(LABEL, LEAD_COPY))

    precise_text <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }

  else if (filter.pobox && in.context == FALSE) {

    precise_text <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(STREET_LABEL = str_extract({{addr.col}}, {{pattern}}),
             LAG_COPY = lag(STREET_LABEL)) %>%
      dplyr::filter(STREET_LABEL == LAG_COPY) %>%
      dplyr::filter(!grepl('^PO BOX|^P.O. BOX|^P O BOX|^P.O.|^POBOX|^P.O|\\bPO BOX\\b', {{addr.col}})) %>%
      dplyr::select(-c(STREET_LABEL, LAG_COPY)) %>%
      dplyr::ungroup()

  }

  else if (filter.pobox == FALSE && in.context) {

    df1 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, {{pattern}}),
             LAG_COPY = lag(LABEL)) %>%
      dplyr::filter(LABEL == LAG_COPY) %>%
      dplyr::select(-c(LABEL, LAG_COPY))

    df2 <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(LABEL = str_extract({{addr.col}}, {{pattern}}),
             LEAD_COPY = lead(LABEL)) %>%
      dplyr::filter(LABEL == LEAD_COPY) %>%
      dplyr::select(-c(LABEL, LEAD_COPY))

    precise_text <- bind_rows(df2, df1) %>%
      dplyr::arrange(across({{arrange.by}})) %>%
      dplyr::ungroup()

  }

  else {

    precise_text <- df %>%
      dplyr::group_by(across({{group.vars}})) %>%
      dplyr::mutate(STREET_LABEL = str_extract({{addr.col}}, {{pattern}}),
             LAG_COPY = lag(STREET_LABEL)) %>%
      dplyr::filter(STREET_LABEL == LAG_COPY) %>%
      dplyr::select(-c(STREET_LABEL, LAG_COPY)) %>%
      dplyr::ungroup()

  }

  cat('Found', nrow(precise_text), 'similar text values.')

  return(precise_text)

}
