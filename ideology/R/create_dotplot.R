library(dplyr)
library(tidyverse)
library(gridExtra)
library(stringr)
library(dotenv)

load_dot_env('../../env')
load_dot_env(Sys.getenv("PATH_TO_SECRETS"))

source("funcs_theme.R")

# Load ggum scores

ggum_scores_house = readRDS("../.tmp/output/H118-estimates-house.rds") |>
  select(icpsr,ggum_dim1)

ggum_scores_senate = readRDS("../.tmp/output/H118-estimates-senate.rds") |>
  select(icpsr,ggum_dim1)

gggum_ideal_points <- rbind(ggum_scores_house,ggum_scores_senate) |>
  mutate(icpsr = as.integer(icpsr),
         ggum_dim1 = round(ggum_dim1, digits = 3))

con <- DBI::dbConnect(
   RMariaDB::MariaDB(), 
   host = "127.0.0.1",
   user = Sys.getenv("DB_USER"),
   password = Sys.getenv("DB_PASSWORD"),
   dbname = 'elite',
   port = Sys.getenv("DB_PORT")
)

lnames <- tbl(con, sql("SELECT first_name, last_name, full_name, bioguide_id from elite.legislators"))  %>% collect()

# 118th Congress: bioguide and 
congress = 118
dta_118 = read.csv('../.tmp/voteview.csv')
dta_118 = dta_118[dta_118$chamber %in% c('House', 'Senate'),]
dta_118 = dta_118 |> 
  left_join(lnames, join_by(bioguide_id)) |>
  left_join(gggum_ideal_points, join_by(icpsr)) |> 
  filter(!is.na(ggum_dim1)) |>
   filter(!is.na(full_name))

dta_118[is.na(dta_118$ggum_dim1),]

# saveRDS(dta_118,"data/ideal_points_house.RDS")

table(is.na(dta_118$full_name))

wrapper <- function(x, width=30) 
{
  paste(strwrap(x, width), collapse = "\n")
}

# Function takes plot data, returns mean of interval where mean r/d/person resides
mean_binwidth = \(pdat, r, d, o){
  pdat = pdat |> 
    distinct(xmin, xmax)
  map_dbl(c(r,d,o), \(p){
    pdat |> 
      filter(xmin <= p & xmax > p) |> 
      mutate(x = (xmin + xmax) / 2) |> 
      slice_head(n = 1) |>  # handles border cases
      pull(x)
  })
}

plot_dots = \(member_name, memberid, data){
  # Member Details
  cham = data$chamber[data$bioname == member_name & data$bioguide_id == memberid]
  ideo = data$ggum_dim1[data$bioname == member_name & data$bioguide_id == memberid]
  full_name = data$full_name[data$bioguide_id == memberid]
  # Min/Max
  ideo_min = min(data$ggum_dim1[data$chamber == cham])
  ideo_max = max(data$ggum_dim1[data$chamber == cham])
  mean_dem = mean(data$ggum_dim1[data$chamber == cham & data$party_code == 100])
  mean_rep = mean(data$ggum_dim1[data$chamber == cham & data$party_code == 200])

  # Independents
  if(n_distinct(data$party_code[data$chamber == cham]) > 2){
    col_val = c("dodgerblue3","firebrick3","darkorchid3","black")
  } else {
    col_val = c("dodgerblue3","firebrick3","black")
  }
  # Plot
  p_pre = data |> 
    filter(chamber == cham) |> 
    mutate(party_code = ifelse(bioname == member_name, 999, party_code)) |> 
    ggplot(aes(x = ggum_dim1, 
               group = as.factor(party_code), 
               fill = as.factor(party_code),
               color = as.factor(party_code))) +
    
    geom_dotplot(method = 'histodot', binwidth = .13, show.legend = F, stackratio = 1.2, dotsize=.7,
                 aes(stroke="white"), alpha=.7) +

    scale_fill_manual(values = col_val) +
    theme_prl()+
    theme(
          axis.text.y = element_blank()) +
    labs(x = NULL,
         y = NULL) +
    scale_x_continuous(breaks=c(ideo_min+.05, ideo_max-.55), labels=c("Most Liberal", "Most Conservative"), expand = expansion(mult = c(0.1, 0.1)))
  
  int_dat = mean_binwidth(ggplot_build(p_pre)$data[[1]], mean_rep, mean_dem, ideo)
  
  p_pre +
    geom_vline(xintercept = int_dat[2], color="dodgerblue3", linetype="dotted", size=.2) +
    geom_vline(xintercept = int_dat[1], color="firebrick3", linetype="dotted", size=.2) +
    geom_curve(
      curvature = sign(ideo)*-.1,
      color="black",
      aes(x = 0, y = .8, xend = int_dat[3], yend = .029),
      arrow = arrow(type="closed", length = unit(.1, "cm")),
      arrow.fill="black", 
      show.legend = FALSE
    ) +
    annotate("label", x = 0, y = .8, label = wrapper(str_to_title(full_name)),
             hjust = .5, fill="white") +
    annotate("label", x = mean_rep, y = .975, label = "Average Republican",
             hjust = .5, fill="firebrick3", color="white") +
    annotate("label", x = mean_dem, y = .975, label = "Average Democrat",
             hjust = .5, fill="dodgerblue3", color="white")
}

# # Example: Kim Schrier (D-WA8)
# p<-plot_dots("BUCK, Kenneth Robert","B001297",dta_118) |> 
#     ggsave('test.png', plot = _, width=4.25, height=4, units="in", dpi=600)
# q()
# p

# Loop Over all Congresspeople 
walk(1:nrow(dta_118), \(i){
  bname = dta_118$bioname[i]
  bguide = dta_118$bioguide_id[i]
  bfile = paste0("../.plots/",bguide,".png")
  plot_dots(bname, bguide, dta_118) |> 
    ggsave(bfile, plot = _, width=4.25, height=4, units="in", dpi=600)
})
