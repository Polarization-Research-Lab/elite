library(tidyverse)
library(ggrepel)
library(ggpmisc)

source(here("ideology","funcs_theme.R"))

ideal_points = readRDS(here("ideology_ggum/data","ideal_points_house.RDS"))

corr <- ggplot(data = ideal_points, aes(nominate_dim1, ggum_dim1, label = last_name)) +
  geom_point() + 
  xlab("NOMINATE") +
  ylab("GGUM") +
  facet_grid(~chamber) +
  stat_dens2d_filter(geom = "text_repel", keep.fraction = 0.1) +
  theme_prl()

corr

ggsave(plot = corr, file = here("ideology_ggum","correlation.png"),dpi = 500)
