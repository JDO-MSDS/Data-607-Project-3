# ================================================================
# DATA 607 Project 3 — SQLite Loader
# Author: Kevin Martin
# Description:
#   Loads the cleaned Google Trends dataset (data/trends_long.csv)
#   into a normalized SQLite warehouse (data/warehouse.db)
#   following the shared team schema: Skill, TrendQuery, TrendPoint.
# ================================================================

# ---- Libraries ----
library(readr)
library(dplyr)
library(DBI)
library(RSQLite)
library(stringr)

# ---- Input / Output paths ----
infile  <- "data/trends_long.csv"    # cleaned CSV created by Joao's Rmd
outfile <- "data/warehouse.db"       # shared SQLite database

# ---- Step 1: Read the data ----
df <- read_csv(infile, show_col_types = FALSE)

if (!all(c("date", "skill_name", "region", "interest") %in% names(df))) {
  stop("Unexpected column structure in trends_long.csv. 
       Expected: date, skill_name, region, interest.")
}

# ---- Step 2: Normalize & clean ----
tidy_df <- df %>%
  rename(skill = skill_name,
         geo   = region,
         interest = interest) %>%
  mutate(
    interest = as.numeric(interest),
    geo = ifelse(is.na(geo), "US", geo),
    time_window  = "past_5_years",
    granularity  = "weekly",
    retrieved_on = as.character(Sys.Date())
  )

# ---- Step 3: Create or connect to database ----
fs::dir_create(dirname(outfile))
con <- dbConnect(SQLite(), outfile)

# ---- Step 4: Ensure required tables exist ----
dbExecute(con, "CREATE TABLE IF NOT EXISTS Skill (
  skill_id INTEGER PRIMARY KEY,
  skill_name TEXT UNIQUE NOT NULL
);")

dbExecute(con, "CREATE TABLE IF NOT EXISTS TrendQuery (
  query_id INTEGER PRIMARY KEY,
  skill_id INTEGER NOT NULL,
  region TEXT NOT NULL,
  time_window TEXT NOT NULL,
  granularity TEXT NOT NULL,
  retrieved_on DATE NOT NULL,
  FOREIGN KEY(skill_id) REFERENCES Skill(skill_id)
);")

dbExecute(con, "CREATE TABLE IF NOT EXISTS TrendPoint (
  point_id INTEGER PRIMARY KEY,
  query_id INTEGER NOT NULL,
  date DATE NOT NULL,
  interest_score REAL NOT NULL,
  FOREIGN KEY(query_id) REFERENCES TrendQuery(query_id)
);")

# ---- Step 5: Insert / update Skill table ----
skills_df <- tidy_df %>% distinct(skill) %>% arrange(skill)
existing_skills <- dbGetQuery(con, "SELECT skill_id, skill_name FROM Skill;")
to_insert <- anti_join(skills_df, existing_skills, by = c("skill" = "skill_name"))

if (nrow(to_insert) > 0) {
  dbWriteTable(con, "Skill",
               to_insert %>% rename(skill_name = skill),
               append = TRUE)
}

skill_dim <- dbGetQuery(con, "SELECT skill_id, skill_name FROM Skill;")

# ---- Step 6: Insert / update TrendQuery table ----
query_df <- tidy_df %>%
  distinct(skill, geo, time_window, granularity, retrieved_on) %>%
  inner_join(skill_dim, by = c("skill" = "skill_name")) %>%
  transmute(skill_id, region = geo, time_window, granularity, retrieved_on)

existing_query <- dbGetQuery(con, "SELECT skill_id, region, time_window, granularity, retrieved_on FROM TrendQuery;")
query_new <- anti_join(query_df, existing_query,
                       by = c("skill_id","region","time_window","granularity","retrieved_on"))

if (nrow(query_new) > 0) {
  dbWriteTable(con, "TrendQuery", query_new, append = TRUE)
}

query_dim <- dbGetQuery(con, "SELECT query_id, skill_id, region FROM TrendQuery;")

# ---- Step 7: Insert new TrendPoint rows ----
points_df <- tidy_df %>%
  inner_join(skill_dim, by = c("skill" = "skill_name")) %>%
  inner_join(query_dim, by = c("skill_id", "geo" = "region")) %>%
  transmute(query_id, date = as.Date(date), interest_score = interest)

existing_points <- dbGetQuery(con, "SELECT query_id, date FROM TrendPoint;") %>%
  mutate(date = as.Date(date))
points_new <- anti_join(points_df, existing_points, by = c("query_id","date"))

if (nrow(points_new) > 0) {
  dbWriteTable(con, "TrendPoint", points_new, append = TRUE)
}

cat("✅ Wrote/updated tables in data/warehouse.db\n")
cat("Tables:", paste(dbListTables(con), collapse = ", "), "\n")

# ---- Optional: quick verification ----
print(head(dbReadTable(con, "TrendPoint")))

# ---- Clean disconnect ----
dbDisconnect(con)
cat("🎯 Done. Database updated successfully.\n")
