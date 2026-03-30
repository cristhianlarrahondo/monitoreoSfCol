################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Función para hacer el pull de la información

## Tasas de interés
## Desembolsos
## Carteras
## Recursos

################################################################################

# Validar si la información de td que es semanal está completa
check_mes_completo = function(fetched_dates, year, month) {
  
  # Primer día de cada mes
  first_day = as.Date(sprintf("%04d-%02d-01", year, month))
  # Último día de cada mes
  last_day = as.Date(format(first_day + 31, "%Y-%m-01")) - 1
  # Genera todos los días del mes
  all_days = seq.Date(first_day, last_day, by = "day")
  
  # Identificar los días viernes (cuando reporta la SFC)
  fridays = all_days[as.POSIXlt(all_days)$wday == 5]
  
  # Fechas traidas desde las API
  fetched_dates = as.Date(fetched_dates)
  
  # Identifica si faltan semanas
  missing_fridays = setdiff(fridays, fetched_dates)
  
  # Validación
  if (length(missing_fridays) == 0) {
    
    cli::cli_alert_success(
      "
      ¡La data descargada tiene todas las semanas!
      "
    )
    return(TRUE)
    
  } else {
    print(cli::rule(center = "❌ ¡CUIDADO", line = "=", col = "#C30909"))
    
    cli::cli_alert_danger(
      "Faltan semanas en el mes: {missing_fridays}"
    )
    return(FALSE)
  }
}

################################################################################
# Función para hacer el pull de la información para cada mes
pull_data = function(
    url, year = NA, month = NA, api_token, type = "", save = "y"
    ) {
  
  ##############################################################################
  
  # Toda la muestra, sin filtro por fecha
  if (is.na(month) & is.na(year)) {
    # Definiendo el filtro basado en fecha
    date_filter = ""
  } else {
    
    # Fecha de inicio
    start_date = glue("{year}-{sprintf('%02d', month)}-01T00:00:00.000")
    # Fecha de fin
    end_date = glue(
      "{year}-{sprintf('%02d', month)}-{format(as.Date(start_date) + 
    days(days_in_month(as.Date(start_date))) - 1, '%d')}T23:59:59.999"
    )
    
    # Definiendo el filtro basado en fecha
    date_filter = URLencode(
      glue("$where=fecha_corte between '{start_date}' and '{end_date}'")
    ) 
  }
  
  # URL completa
  url_to_request = paste0(url, date_filter, '&$limit=100000000')
  
  # Archivo temporal para guardar la respuesta
  tmp_file = tempfile(fileext = ".json")
  
  # Hacer el request de la información y escribir el archivo
  response = GET(
    url = url_to_request,
    add_headers(`X-App-Token` = api_token),
    write_disk(tmp_file, overwrite = TRUE)
  )
  
  ##############################################################################
  
  # Revisar el estado del request
  if (status_code(response) == 200) {
    
    # Si el request sale bien, guarda el dataframe
    data = fromJSON(tmp_file)
    
    # Para dc, fecha y mes
    if (type == "dc") {
      year = lubridate::year(max(as.Date(data$fecha_corte)))
      month = lubridate::month(max(as.Date(data$fecha_corte)))
    }
    
    if (save == "y" & type != "") {
      # Guardar el .rds
      saveRDS(
        object = data,
        file = glue(
          "0_data/0_raw/pulled/{type}/raw_{type}_{year}_{sprintf('%02d', month)}.rds"
        )
      ) 
    }
    
    # Validar para td que esté el mes completo (todas las semanas)
    if (type == "td") {
      check_mes_completo(
        fetched_dates = unique(data$fecha_corte),
        year = year, 
        month = month
      )
    }
    
    return(data)
    
  } else { # Si no sale bien, entonces un mensaje de alerta
    warning(
      glue(
        "Failed to fetch data for {year}-{month}. 
         Status code: {status_code(response)}"
      )
    )
    return(NULL)
  }
}
################################################################################

