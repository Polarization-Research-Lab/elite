theme_prl <- function(base_size = 12, base_family = "sans") {
  colors <- deframe(ggthemes::ggthemes_data[["fivethirtyeight"]])
  (theme_foundation(base_size = base_size, base_family = base_family)
    + theme(
      line = element_line(colour = "black"),
      rect = element_rect(
        linetype = 0, colour = NA),
      text = element_text(colour = colors["Dark Gray"]),
      axis.title = element_text(size = rel(.8)),
      axis.text = element_text(color = colors["Dark Gray"],face = "bold"),
      axis.ticks = element_blank(),
      axis.line = element_blank(),
      legend.background = element_rect(),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.box = "vertical",
      panel.grid = element_line(colour = NULL),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.background = element_rect(fill = NA),
      plot.title = element_text(hjust = 0, size = rel(1.5), face = "bold"),
      plot.margin = unit(c(.5, .5, .5, .5), "lines"),
      strip.text = element_text(face = "bold"),
      plot.caption = element_text(),
      strip.background = element_rect()) 
  )
}

library(ggthemes)

theme_prl_tick <- function(base_size = 12, base_family = "sans") {
  colors <- deframe(ggthemes::ggthemes_data[["fivethirtyeight"]])
  (theme_foundation(base_size = base_size, base_family = base_family)
    + theme(
      line = element_line(colour = "black"),
      rect = element_rect(
        linetype = 0, colour = NA),
      text = element_text(colour = colors["Dark Gray"]),
      axis.title = element_text(size = rel(.8)),
      axis.text = element_text(color = colors["Dark Gray"],face = "bold"),
      axis.ticks.x = element_blank(),
      axis.line = element_blank(),
      legend.background = element_rect(),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.box = "vertical",
      panel.grid = element_line(colour = NULL),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      plot.title = element_text(hjust = 0, size = rel(1.5), face = "bold"),
      plot.margin = unit(c(1, 1, 1, 1), "lines"),
      strip.text = element_text(face = "bold"),
      plot.caption = element_text(),
      strip.background = element_rect()) 
  )
}