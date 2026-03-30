################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Función auxiliar

## Cifras en millones

################################################################################

# Carteras y recursos
cr_function = function(data) {
  
  ##############################################################################
  # Manejo de los archivos nuevos y respaldos
  ## Ruta del cr_mas_reciente actual
  current_path = "0_data/1_processed/mas_reciente/cr/cr_mas_reciente.rds"
  # Ruta donde se guarda el respaldo
  backup_dir = "0_data/1_processed/mas_reciente/cr/respaldo"
  file_pattern = "cr_mas_reciente_\\d{4}_\\d{2}\\.rds"
  
  # Archivos a borrar del respaldo
  files_to_delete = dir_ls(backup_dir, regexp = file_pattern) %>%
    tibble(path = .) %>%
    mutate(
      name = path_file(path),
      date = as.Date(paste0(str_extract(name, "\\d{4}_\\d{2}"), "_01"), "%Y_%m_%d")
    ) %>%
    arrange(desc(date)) %>%
    slice(-1:-3) %>%   # everything *except* the 3 most recent
    pull(path)
  
  ##############################################################################
  # Cargando archivos para el procesamiento
  
  # Archivo histórico más reciente
  cr_mas_reciente = readRDS(
    file = glue("{current_path}")
  )
  
  # Fecha del mes a cargar
  fetched_date = max(unique(floor_date(ymd_hms(data$fecha), "month")))
  
  # Fecha más reciente del archivo histórico
  mostrecent_date = max(unique(cr_mas_reciente$fecha))
  
  ##############################################################################
  
  # Validaciones del proceso
  ## Que data (la traida de la API) sólo tenga un mes
  if (length(fetched_date) == 1) {
    
    ## Que la fecha de la data traida sea superior a la más reciente ya consolidada
    if (fetched_date > mostrecent_date) {
      
      expected_next_date = mostrecent_date %m+% months(1)
      
      ## Que además sea justamente el mes siguiente
      if (fetched_date == expected_next_date) {
       
        ########################################################################
        
        cli::cli_alert_success(
          "¡El mes descargado se actualizará! Continuando con el proceso..."
        )
        
        # Códigos CUIF
        cr_codes = readxl::read_excel(
          path = "0_data/0_raw/diccionarios/diccionario_cr.xlsx", 
          sheet = "diccionario"
        )
        
        # Sufijo para guardar archivos del mes
        suffix_filename = glue(
          "_{lubridate::year(expected_next_date)}_{sprintf('%02d',lubridate::month(expected_next_date))}"
        )
        
        # Nombre y carpeta del archivo cr más reciente guardado de respaldo
        # Antes que la actualización reemplace el actual
        # cr_mas_reciente.rds
        backup_filename = glue(
         "cr_mas_reciente{suffix_filename}.rds" 
        )
        backup_path = file.path(backup_dir, backup_filename)
        
        ########################################################################
        
        cli::cli_alert_success(
          "Empezando con el tratamiento de los datos..."
        )
        
        # Procesando la información
        cufi_m = data %>% 
          filter(
            # Moneda total
            moneda == "0",
            nombre_tipo_entidad != "INSTITUCIÓN OFICIAL ESPECIAL",
            tipo_entidad %in% c("1", "2", "4", "32")
          ) %>%
          mutate(across(c(cuenta, valor), as.numeric)) %>% 
          mutate(
            fecha = as.Date(floor_date(ymd_hms(fecha_corte), "month"))
          ) %>% 
          left_join(y = cr_codes, by = join_by(cuenta)) %>% 
          filter(!is.na(macrocuenta)) %>% 
          group_by(fecha, nombre_entidad, tipo, macrocuenta) %>% 
          reframe(
            # En millones
            # Para calzar con el formato de IG de la super
            valor = sum(valor, na.rm = T)/1e6 
          ) %>% ungroup() %>% 
          pivot_wider(names_from = c(tipo, macrocuenta), values_from = valor) %>% 
          mutate(
            Vencida_Total = Vencida_Comercial + Vencida_Consumo + Vencida_Hipotecaria + Vencida_Microcrédito,
            Recursos_Total = Recursos_CDT + Recursos_Vista
          ) %>% 
          pivot_longer(
            cols = -c(fecha:nombre_entidad), 
            names_to = c("tipo", "macrocuenta"), 
            names_sep = "_",
            values_to = "valor", 
            values_drop_na = T
          )
        
        # Guardando el archivo procesado del mes
        cli::cli_alert_success(
          "
          Archivo raw del mes {.file cr{suffix_filename}.rds} guardado en {.path 0_data/1_processed/mensual/cr/}
          "
        )
        
        saveRDS(
          object = cufi_m, 
          file = glue("0_data/1_processed/mensual/cr/cr{suffix_filename}.rds")
        )
        
        ########################################################################
        # Cálculos intermedios
        
        total_entidad = cufi_m %>% 
          group_by(fecha, tipo, macrocuenta) %>% 
          reframe(
            valor = sum(valor, na.rm = T)
          ) %>% 
          mutate(
            nombre_entidad = "Total"
          ) %>% ungroup() %>% 
          pivot_wider(names_from = c(tipo, macrocuenta), values_from = valor) %>% 
          mutate(
            Vencida_Total = Vencida_Comercial + Vencida_Consumo + Vencida_Hipotecaria + Vencida_Microcrédito,
            Recursos_Total = Recursos_CDT + Recursos_Vista
          ) %>% 
          pivot_longer(
            cols = -c(fecha:nombre_entidad), 
            names_to = c("tipo", "macrocuenta"), 
            names_sep = "_",
            values_to = "valor", 
            values_drop_na = T
          )
        
        cufi_t = bind_rows(cr_mas_reciente, cufi_m, total_entidad) %>% 
          arrange(fecha, tipo, macrocuenta, nombre_entidad)
        
        # Moviendo el archivo más reciente a la carpeta de respaldo 
        if (file_exists(current_path)) {
          file_move(current_path, backup_path)
          
          cli::cli_alert_info(
            "Antiguo archivo {.file cr_mas_reciente.rds} guardado como respaldo en: {.path 0_data/1_processed/mas_reciente/cr/respaldo}"
          )
          
        } else {
          cli::cli_alert_warning("No existe un archivo {.file cr_mas_reciente.rds} para respaldar.")
        }
        
        # Guardando la nueva versión más reciente
        saveRDS(object = cufi_t, current_path)
        print(cli::rule(center = "🎉 ¡Los datos han sido actualizados!", line = "=", col = "green"))
        cli::cli_alert_success(
          "El archivo nuevo {.file cr_mas_reciente.rds} guardado en: {.path {current_path}}"
        )
        
        # Eliminando respaldos antiguos (solo se dejan los últimos 3 meses)
        file_delete(files_to_delete)
        
        # RETURN
        return(cufi_t)
         
      } else {
        
        print(cli::rule(
          center = "🚧 ¡CUIDADO! La fecha de la data traida es mayor a la más reciente. Pero hay meses atrás pendientes por actualizar", 
          col = "#FFC552"
        ))
        
        cli::cli_alert_danger(
          "
          Revisar la fecha de la data traida. 
          Aunque es mayor a la más reciente en el archivo histórico, la cual es: \n
          {mostrecent_date} \n
          Debe ser el mes siguiente, es decir: \n
          {expected_next_date}, pero es {fetched_date}
          "
        )
        
      }
      
    } else if (fetched_date == mostrecent_date) { # La fecha traida es igual a la más reciente
      
      print(cli::rule(center = "💻 ¡Los datos ya estaban actualizados!", line = "=", col = "#FFC552"))
      
      cli::cli_alert_warning(
        "
        La data ya está actualizada a la fecha:
        {mostrecent_date}
        No se modificará ningún archivo y sólo se cargará el dataframe en una lista.
        "
      )
      
      cufi_t = readRDS(
        file = glue("{current_path}")
      )
      
    } else {
      print(cli::rule(center = "🚧 ¡CUIDADO! La fecha de la data traida es menor a la más reciente.", col = "#FFC552"))
      
      cli::cli_alert_danger(
        "
        Revisar la fecha de la data traida. 
        Debe ser el mes siguiente a la más reciente en el archivo histórico, la cual es: 
        {mostrecent_date}, pero es {fetched_date}
        "
      )
    }
  } else {
    print(cli::rule(center = "❌ ¡ERROR!", line = "=", col = "#C30909"))
    
    cli::cli_alert_danger(
      "La data traida debe tener solo un mes, no más de uno a la vez."
    )
  }
  
}
