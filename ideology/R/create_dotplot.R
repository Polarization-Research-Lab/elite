library(dplyr)
library(tidyverse)
library(gridExtra)
library(stringr)
library(dotenv)

source("funcs_theme.R")

lnames <- read.csv('../.tmp/legislators.csv')

dta_data = read.csv('../.tmp/voteview.csv')
dta_data = dta_data[dta_data$chamber %in% c('House', 'Senate'),]
dta_data = dta_data |> 
  left_join(lnames, join_by(bioguide_id)) |>
  filter(!is.na(nominate_dim1))

# table(is.na(dta_data$full_name))

dta_data$full_name <- paste(dta_data$first_name, dta_data$last_name, sep = " ")

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
  cham = data$chamber[data$bioguide_id == memberid]
  ideo = data$nominate_dim1[data$bioguide_id == memberid]
  full_name = data$full_name[data$bioguide_id == memberid]
  # Min/Max
  ideo_min = min(data$nominate_dim1[data$chamber == cham])
  ideo_max = max(data$nominate_dim1[data$chamber == cham])
  mean_dem = mean(data$nominate_dim1[data$chamber == cham & data$party_code == 100])
  mean_rep = mean(data$nominate_dim1[data$chamber == cham & data$party_code == 200])

  # Independents
  if(n_distinct(data$party_code[data$chamber == cham]) > 2){
    col_val = c("dodgerblue3","firebrick3","darkorchid3","black")
  } else {
    col_val = c("dodgerblue3","firebrick3","black")
  }

  dotsize <- ifelse(cham == "House", 0.5, 0.7)

  # Plot
  p_pre = data |> 
    filter(chamber == cham) |> 
    mutate(party_code = ifelse(bioname == member_name, 999, party_code)) |> 
    ggplot(aes(x = nominate_dim1, 
               group = as.factor(party_code), 
               fill = as.factor(party_code),
               color = as.factor(party_code))) +
    
    geom_dotplot(method = 'histodot', binwidth = .05, show.legend = F, stackratio = 1, dotsize=dotsize,
                 aes(stroke="white"), alpha=.7) +

    scale_fill_manual(values = col_val) +
    theme_prl()+
    theme(
          axis.text.y = element_blank()) +
    labs(x = NULL,
         y = NULL) +
    scale_x_continuous(breaks=c(ideo_min, ideo_max), labels=c("Most Liberal", "Most Conservative"), expand = expansion(mult = c(0.1, 0.1)))
  
  int_dat = mean_binwidth(ggplot_build(p_pre)$data[[1]], mean_rep, mean_dem, ideo)
  # Extract y-axis max dynamically from ggplot data
  p_data <- ggplot_build(p_pre)$data[[1]]
  # y_max <- max(p_data$count, na.rm = TRUE)  # Use max count for scaling
  y_max <- 150

  p_pre +
      geom_vline(xintercept = int_dat[2], color="dodgerblue3", linetype="dotted", linewidth=.2) +
      geom_vline(xintercept = int_dat[1], color="firebrick3", linetype="dotted", linewidth=.2) +
      geom_curve(
        curvature = sign(ideo) * -0.1,
        color = "black",
        aes(x = 0, y = .8, xend = int_dat[3], yend = 0.029),  # Auto-scaled Y
        arrow = arrow(type="closed", length = unit(.1, "cm")),
        arrow.fill = "black", 
        show.legend = FALSE
      ) +
      annotate("label", x = 0, y = .8, label = wrapper(str_to_title(full_name)), 
               hjust = .5, fill = "white") +
      annotate("label", x = mean_rep, y = 0.975, label = "Average Republican",
               hjust = .5, fill = "firebrick3", color = "white") +
      annotate("label", x = mean_dem, y = 0.975, label = "Average Democrat",
               hjust = .5, fill = "dodgerblue3", color = "white")
}


# SENATE EXAMPLE
# p<-plot_dots("fdjsklfdjsklfjdskl","W000805",dta_data) |> 
#     ggsave('test-sen.png', plot = _, width=6.25, height=4, units="in", dpi=600)
# # REPRESENTATIVE EXAMPLE
# p<-plot_dots("fdjsklfdjsklfjdskl","B000490",dta_data) |> 
#     ggsave('test-rep.png', plot = _, width=6.25, height=4, units="in", dpi=600)
# quit()

# Loop Over all Congresspeople 
walk(1:nrow(dta_data), \(i){
  bname = dta_data$bioname[i]
  bguide = dta_data$bioguide_id[i]
  bfile = paste0("../.plots/", bguide, ".png")
  
  # Check if data exists before proceeding
  if (is.na(bname) || is.na(bguide)) {
    message("Skipping ", i, ": Missing bioname or bioguide_id")
    return(NULL)  # Skip this iteration
  }

  plot <- tryCatch({
    plot_dots(bname, bguide, dta_data)
  }, error = function(e) {
    message("Skipping ", i, ": Error encountered - ", e$message)
    return(NULL)  # Skip if plot_dots fails
  })
  
  if (!is.null(plot)) {
    ggsave(bfile, plot = plot, width = 6.25, height = 4, units = "in", dpi = 600)
  }

})
