require(transformeR)

suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
    require(ggplot2)
})


##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#' @title Get available models
#' @description Yields a vector of model names with complete hourly inputs
#' @param plot Logical: should a matrix-like plot be displayed with the results? (default to FALSE)
#' @return A character vector with model IDs containing all required variables (see Details)
#' @details 
#' Complete hourly inputs are models that already have uploaded 1-hourly evaluation simulations of 'hurs', 'tas', 'sfcWind' and 'pr'.
#' The function remotely reads from GitHub's repo joint-evaluation catalog.csv (devel branch) and yields an overview of available hourly variables for FWI calculation.
#' @imporFrom magrittr %>%
#' @importFrom dplyr group_by distinct
#' @importFrom rlang syms
#' @importFrom ggplot2 ggplot
#' @keywords internal
#' @author juaco

availableModels <- function(do.plot = FALSE) {
    stopifnot(is.logical(do.plot))
    url <- "https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/catalog.csv"
    data <- read.csv(url)
    hourly.fwi <- subset(data,
                     subset = frequency %in% "1hr" & variable_id %in% c("hurs", "tas", "sfcWind", "pr"))
    # Grouped summary
    hf <- hourly.fwi %>% group_by(institution_id, driving_source_id, driving_experiment_id, source_id) 
    ids <- c("project_id", "mip_era", "activity_id", "domain_id", "institution_id",
         "driving_source_id", "driving_experiment_id", "driving_variant_label",
         "source_id", "version_realization", "frequency", "variable_id")
    ## Unique rows (skip file path and variable version info)
    hf1 <- distinct(hf, !!!syms(ids)) %>% as.data.frame()
    # Binary matrix
    ## Label vars as 'var_id-1hr'
    hf1[["var_freq"]] <- paste(hf1$variable_id, hf1$frequency, sep = "-")
    binary_matrix <- table(hf1$var_freq, hf1$source_id)
    if (do.plot) {
        # matrix as data.frame (for plotting)
        binary_df <- as.data.frame(as.table(binary_matrix))
        graph <- ggplot(binary_df, aes(Var1, Var2)) +
        geom_tile(aes(fill = Freq), color = "gray30") +
        scale_fill_gradient(low = "white", high = "darkblue", guide = "none") +
        labs(x = NULL, y = NULL, fill = "Frequency",
             title = paste("Checked on", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))) +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 12),
              axis.text.y = element_text(size = 12))
        print(graph)
        }
    which(colSums(binary_matrix) == 4L) %>% names()
}

##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#' @title Precip accumulation from 12 to 12
#' @description Accumulates precipitation from 12UTC to 12UTC, as required for FWI calculation in EUR domain
#' @param grid A valid climate4R hourly grid of precipitation
#' @return A daily accumulated precipitation grid, with adjusted metadata
#' @imporFrom transformeR isGrid getVarNames array3Dto2Dmat mat2Dto3Darray
#' @keywords internal
#' @author J. Bedia

accum_pr <- function(grid) {
    if (!isGrid(grid)) {
        stop("Not a valid input grid")
    }
    varname <- getVarNames(grid)
    if (varname != "pr") {
        warning("Variable name is ", varname, "(should be daily accumulable)")
    }
    t.res <- getTimeResolution(grid)
    if (t.res != "1h")  {
        stop("Input grid must have hourly time frequency")
    }
    ## Vector of ref dates
    dates <- as.POSIXct(grid$Dates$start)
    ## Extract hour from dates vector
    h <- format(dates, "%H")
    ## Identify 12:00 UTC records
    ind <- which(h == 12)
    ## Remove incomplete 12-12 days
    dates.adj <- dates[(min(ind)+1):max(ind)]
    ## aggregation index for accumulation
    INDEX <- format(dates.adj - 3600*13, "%j")
    ## Convert array to matrix
    mat <- array3Dto2Dmat(grid$Data)
    mat <- mat[(min(ind)+1):max(ind),]
    ## Aggregation 12-12
    aggr.mat <- apply(mat, MARGIN = 2, FUN = function(x) {
        tapply(x, INDEX = INDEX, FUN = sum, na.rm = TRUE)
    })
    mat <- NULL
    gc()
    ## Recover array structure
    grid$Data <- mat2Dto3Darray(aggr.mat, x = grid$xyCoords$x, y = grid$xyCoords$y)
    ## Update attributes
    attr(grid$Variable, "daily_agg_cellfun") <- "sum"
    attr(grid$Variable, "verification_time") <- "12:00 UTC"
    ## Adjust dates
    grid$Dates$end <- dates.adj[seq(24,length(dates.adj),24)]
    grid$Dates$start <- grid$Dates$end - 3600*24
    return(grid)
}

##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

                