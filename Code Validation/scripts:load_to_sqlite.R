# ================================================================
# Project 3: Load Google Trends Data into SQLite
# Author: Kevin Martin
# Description:
#   - Loads cleaned Google Trends data from CSV (from GitHub or local)
#   - Normalizes structure for analysis
#   - Stores results into a local SQLite database
# ================================================================

# ---- Libraries -----
library(readr)
library(dplyr)
library(tidyr)
library(stringr)

# Try to locate the real header line (first line starting with Week/date)
all_lines <- readLines(infile, warn = FALSE)
hdr_idx <- which(grepl("^(Week|week|Date|date),", all_lines))[1]
if (is.na(hdr_idx)) hdr_idx <- 1  # fallback to first line

df_raw <- read_csv(infile, skip = hdr_idx - 1, show_col_types = FALSE)

# Drop wholly empty columns (e.g., Unnamed: 6)
df_raw <- df_raw %>% select(where(~ !all(is.na(.x))))
names(df_raw) <- names(df_raw) |> str_trim()

# ---- 3. Reshape to long format (handles multiple shapes) ----
# Case A: Wide Google Trends export (Week + skill columns)
if (any(tolower(names(df_raw)) == "date") &&
    any(names(df_raw) %in% c("Python","R","SQL","Tableau"))) {
  
  tidy_df <- df_raw %>%
    rename(date = !!names(df_raw)[tolower(names(df_raw)) == "date"][1]) %>%
    pivot_longer(cols = -date, names_to = "skill", values_to = "interest") %>%
    mutate(
      interest    = as.integer(ifelse(interest == "<1", 0, interest)),
      geo         = "US",
      time_window = "past_5_years",
      granularity = "weekly"
    )
  
  # Case B: gtrendsR long format (date/keyword/hits)
} else if (all(c("date","keyword","hits") %in% tolower(names(df_raw)))) {
  
  col_date <- names(df_raw)[tolower(names(df_raw)) == "date"][1]
  col_kw   <- names(df_raw)[tolower(names(df_raw)) == "keyword"][1]
  col_hits <- names(df_raw)[tolower(names(df_raw)) == "hits"][1]
  
  tidy_df <- df_raw %>%
    rename(date = !!col_date, skill = !!col_kw, interest = !!col_hits) %>%
    mutate(
      skill = case_when(
        str_detect(skill, regex("^python",  TRUE)) ~ "Python",
        str_detect(skill, regex("^r(\\s|\\(|$)", TRUE)) ~ "R",
        str_detect(skill, regex("^sql",     TRUE)) ~ "SQL",
        str_detect(skill, regex("^tableau", TRUE)) ~ "Tableau",
        TRUE ~ skill
      ),
      interest    = as.integer(ifelse(interest == "<1", 0, interest)),
      geo         = "US",
      time_window = "past_5_years",
      granularity = "weekly"
    )
  
  # Case C: Already long but with slight name differences (date/skill/interest)
} else if (all(c("date","skill","interest") %in% tolower(names(df_raw)))) {
  
  col_date  <- names(df_raw)[tolower(names(df_raw)) == "date"][1]
  col_skill <- names(df_raw)[tolower(names(df_raw)) == "skill"][1]
  col_int   <- names(df_raw)[tolower(names(df_raw)) == "interest"][1]
  
  tidy_df <- df_raw %>%
    rename(date = !!col_date, skill = !!col_skill, interest = !!col_int) %>%
    mutate(
      interest    = as.integer(ifelse(interest == "<1", 0, interest)),
      geo         = "US",
      time_window = "past_5_years",
      granularity = "weekly"
    )
  
  # None matched — show columns and stop helpfully
} else {
  stop(paste0(
    "Unrecognized CSV shape. Columns detected: ",
    paste(names(df_raw), collapse = ", "),
    "\nTip: If using Google Trends UI, expect 'Week,Python,R,SQL,Tableau' (maybe after a few junk lines). ",
    "If using gtrendsR, expect 'date, keyword, hits'."
  ))
}

# Sanity check
stopifnot(all(c("date","skill","interest") %in% names(tidy_df)))
cat("✅ Parsed columns:", paste(names(tidy_df), collapse = ", "), "\n")
print(head(tidy_df, 6))

# ---- Write to SQLite and verify ----
library(DBI)
library(RSQLite)
con <- dbConnect(SQLite(), "db/trends.sqlite")

# skills dim
skills <- tidy_df |>
  dplyr::select(skill) |>
  dplyr::distinct() |>
  dplyr::mutate(skill_id = dplyr::row_number()) |>
  dplyr::select(skill_id, skill)

# fact table
trend_data <- tidy_df |>
  dplyr::left_join(skills, by = "skill") |>
  dplyr::mutate(
    point_id = dplyr::row_number(),
    date = format(as.Date(date), "%Y-%m-%d")  # <-- ensure ISO text
  ) |>
  dplyr::select(point_id, skill_id, date, interest, geo, time_window, granularity)

DBI::dbWriteTable(con, "skills", skills, overwrite = TRUE)
DBI::dbWriteTable(con, "trend_data", trend_data, overwrite = TRUE)

cat("\n✅ Database tables written successfully!\n")
print(DBI::dbListTables(con))
print(utils::head(DBI::dbReadTable(con, "trend_data")))
DBI::dbDisconnect(con)
cat("\n🎯 Done. Database saved at: db/trends.sqlite\n")
