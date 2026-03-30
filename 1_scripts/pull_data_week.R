pull_data_week <- function(
    url,
    start_date = NA,
    end_date   = NA,
    api_token,
    type = "",
    save = "y",
    file_name = NA,
    align_to_friday = TRUE
) {
  
  # Helpers -------------------------------------------------------------------
  to_date <- function(x) {
    if (is.na(x)[1]) return(NA)
    if (inherits(x, "Date")) return(x)
    as.Date(x)
  }
  
  # Devuelve el viernes (Date) de la semana de una fecha dada:
  # - si ya es viernes, queda igual
  # - si no, mueve hacia adelante hasta el próximo viernes
  next_friday <- function(d) {
    # wday: Sunday = 1 ... Saturday = 7 (lubridate default)
    wd <- lubridate::wday(d)
    offset <- (6 - wd) %% 7  # Friday = 6
    d + lubridate::days(offset)
  }
  
  # Devuelve el viernes (Date) anterior o igual
  prev_friday <- function(d) {
    wd <- lubridate::wday(d)
    offset <- (wd - 6) %% 7
    d - lubridate::days(offset)
  }
  
  # Parse fechas --------------------------------------------------------------
  start_date <- to_date(start_date)
  end_date   <- to_date(end_date)
  
  # Si no hay rango, trae toda la muestra
  if (is.na(start_date)[1] || is.na(end_date)[1]) {
    date_filter <- ""
  } else {
    
    # Normalizar orden por si vienen invertidas
    if (end_date < start_date) {
      tmp <- start_date
      start_date <- end_date
      end_date <- tmp
    }
    
    # Alinear a viernes si aplica (cortes semanales)
    if (isTRUE(align_to_friday)) {
      start_date <- next_friday(start_date)
      end_date   <- prev_friday(end_date)
    }
    
    # Si después de alinear el rango queda vacío
    if (!is.na(start_date)[1] && !is.na(end_date)[1] && end_date < start_date) {
      warning("Rango inválido luego de alinear a viernes: no hay viernes en el intervalo.")
      return(NULL)
    }
    
    # Formato timestamp como el que usabas
    start_ts <- paste0(format(start_date, "%Y-%m-%d"), "T00:00:00.000")
    end_ts   <- paste0(format(end_date,   "%Y-%m-%d"), "T23:59:59.999")
    
    date_filter <- URLencode(
      glue::glue("$where=fecha_corte between '{start_ts}' and '{end_ts}'")
    )
  }
  
  # URL completa --------------------------------------------------------------
  url_to_request <- paste0(url, date_filter, "&$limit=100000000")
  
  # Guardar response crudo (JSON) en carpeta final/request --------------------
  request_dir <- file.path("0_data", "2_final", "request")
  if (!dir.exists(request_dir)) dir.create(request_dir, recursive = TRUE)
  
  # Si no se da file_name y save == "y", crear uno por defecto
  if (isTRUE(save == "y")) {
    
    if (is.na(file_name)[1] || !nzchar(file_name)) {
      
      if (!is.na(start_date)[1] && !is.na(end_date)[1]) {
        file_name <- glue::glue(
          "request_{format(start_date,'%Y%m%d')}_{format(end_date,'%Y%m%d')}.rds"
        )
      } else {
        file_name <- "request_all.rds"
      }
    }
    
    # asegurar extensión .rds
    if (!grepl("\\.rds$", file_name, ignore.case = TRUE)) {
      file_name <- paste0(file_name, ".rds")
    }
  }
  
  # Archivo temporal para descargar JSON
  tmp_file <- tempfile(fileext = ".json")
  
  # Request -------------------------------------------------------------------
  response <- httr::GET(
    url = url_to_request,
    httr::add_headers(`X-App-Token` = api_token),
    httr::write_disk(tmp_file, overwrite = TRUE)
  )
  
  # Validación ----------------------------------------------------------------
  if (httr::status_code(response) == 200) {
    
    data <- jsonlite::fromJSON(tmp_file)
    
    if (isTRUE(save == "y")) {
      out_path <- file.path(request_dir, file_name)
      saveRDS(object = data, file = out_path)
    }
    
    return(data)
    
  } else {
    warning(
      glue::glue(
        "Failed to fetch data. Status code: {httr::status_code(response)}"
      )
    )
    return(NULL)
  }
}


