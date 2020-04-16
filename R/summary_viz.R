custom_theme_bw <- function() {
  theme_bw() +
    theme(axis.title = element_text(face = "bold"),
          legend.title = element_text(face = "bold"),
          plot.title = element_text(face = "bold"))
}

plot_counts_by_annotationkey <- function(
  view_df, annotation_keys, replace_missing = "Not Annotated", 
  chart_height = NULL, label = "Files"
) {
  
  chart <- annotation_keys %>%
    map2(.y = names(.), function(annotation_prettykey, annotation_key) {
      key_col <- as.name(annotation_key)
      plot_df <- view_df %>%
        group_by(.dots = annotation_key) %>%
        tally() %>%
        mutate_at(.vars = annotation_key,
                  funs(replace(., is.na(.), replace_missing))) %>%
        mutate(UQ(key_col) := forcats::fct_relevel(
          UQ(key_col), replace_missing, after = 0L
        )) %>%
        mutate(label = glue::glue(
          "<b>{value}:</b>\n{count} {label}s",
          value = UQ(key_col),
          count = n,
          label = str_to_lower(label)
        ))
      
      p <- plot_df %>%
        ggplot(aes(x = 1, y = .data$n, text = .data$label)) +
        geom_col(aes_(fill = as.name(annotation_key)),
                 position = position_stack(reverse = FALSE),
                 colour = "white", size = 0.2) +
        scale_fill_viridis_d() +
        xlab(annotation_prettykey) +
        ylab(glue::glue("Number of {label}s", label = label)) +
        scale_x_continuous(expand = c(0, 0)) +
        scale_y_continuous(expand = c(0, 0)) +
        custom_theme_bw() +
        theme(axis.text.x = element_blank(),
              axis.ticks.x = element_blank(),
              axis.text.y = element_blank(),
              axis.ticks.y = element_blank()) +
        guides(fill = FALSE)
      
      # p
      plotly::ggplotly(p, tooltip = "text",
                       width = 100 * length(annotation_keys) + 50,
                       height = chart_height)
    }) %>%
    plotly::subplot(shareY = TRUE, titleX = TRUE) %>%
    plotly::layout(showlegend = FALSE,
                   font = list(family = "Roboto, Open Sans, sans-serif")) %>%
    plotly::config(displayModeBar = F) %>% 
    I
  chart
}

plot_sankey <- function(df, connector_var) {
  df <- df %>% 
    select(-c(connector_var))
  df <- df %>% 
    mutate_all(fct_infreq) %>% 
    mutate_all(fct_rev)
  p = easyalluvial::alluvial_wide(df, col_vector_flow = ggthemes::colorblind_pal()(8))
  parcats(p, marginal_histograms = FALSE, data_input = df)
}