################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Enero 2026
################################################################################

# Función auxiliar

## Cifras en millones

################################################################################

# Distribución de la cartera
dc_function = function(data) {
  
  ##
  # Manejo de los archivos nuevos y respaldos
  ## Ruta del td_mas_reciente actual
  current_path = "0_data/1_processed/mas_reciente/dc/dc_mas_reciente.RData"
  # Ruta donde se guarda el respaldo
  backup_dir = "0_data/1_processed/mas_reciente/dc/respaldo"
  file_pattern = "dc_mas_reciente_\\d{4}_\\d{2}\\.RData"
  
  # Archivos a borrar del respaldo
  files_to_delete = fs::dir_ls(backup_dir, regexp = file_pattern) %>%
    tibble(path = .) %>%
    mutate(
      name = path_file(path),
      date = as.Date(paste0(str_extract(name, "\\d{4}_\\d{2}"), "_01"), "%Y_%m_%d")
    ) %>%
    arrange(desc(date)) %>%
    slice(-1:-3) %>%   # everything *except* the 3 most recent
    pull(path)
  
  ##############################################################################
  
  # Fecha de la data traida de la API
  fetched_date = max(as.Date(unique(floor_date(ymd_hms(data$fecha_corte), "month"))))
  
  # Histórico más reciente
  env = new.env()
  load(current_path, envir = env)
  
  # Access everything as a list
  dc_mas_reciente = as.list(env)
  
  # Fecha más reciente del archivo histórico
  mostrecent_date = max(unique(dc_mas_reciente[["dc_subproducto"]]$fecha))
  rm(env, dc_mas_reciente)
  
  # Validaciones del proceso
  
  ## Start: Que la fecha traida sea igual a la más reciente
  if (fetched_date == mostrecent_date) {
    print(cli::rule(
      center = "🚧 ¡CUIDADO! La fecha de la data traida es igual a la más reciente. No se hará nada", 
      col = "#FFC552"
    ))
  }
    
  ## Que la fecha de la data traida sea superior a la más reciente ya consolidada
  if (fetched_date > mostrecent_date) {
    ########################################################################
    
    cli::cli_alert_success(
      "¡El mes descargado se actualizará! Continuando con el proceso..."
    )
    
    # Sufijo para guardar archivos del mes
    suffix_filename = glue(
      "_{lubridate::year(mostrecent_date)}_{sprintf('%02d',lubridate::month(mostrecent_date))}"
    )
    
    # Nombre y carpeta del archivo dc más reciente guardado de respaldo
    # Antes que la actualización reemplace el actual
    backup_filename = glue(
      "dc_mas_reciente{suffix_filename}.Rdata" 
    )
    backup_path = file.path(backup_dir, backup_filename)
    
    ########################################################################
    
    cli::cli_alert_success(
      "Empezando con el tratamiento de los datos..."
    )
    
    
    ## Cargando archivos para el procesamiento  
    
    ## Diccionario de los tipos de subproducto
    diccionario = readxl::read_excel(path = "0_data/0_raw/diccionarios/diccionario_dc.xlsx") %>% 
      dplyr::select(1, 4:6) %>% 
      filter(!is.na(modalidad))
    
    ## Listado de entidades
    entidades = readxl::read_excel(path = "0_data/0_raw/diccionarios/entidades_dc.xlsx") %>% 
      dplyr::select(1, 2, 4)
    
    ################################################################################
    
    ## Transformaciones iniciales
    dc_filtered = data %>% 
      # Limpiando nombre de las columnas
      janitor::clean_names() %>% 
      # Filtrando por tipo de entidad
      filter(
        # 1 Bancos, 2 Corporaciones financieras, 22 IOEs, 32 Cooperativas caracter financiero, 4 Compañias de financiamiento
        tipo_entidad %in% c("1", "2", "4", "32")
      ) %>%
      mutate(
        # Creando fecha mensual (datos son semanales)
        fecha = as.Date(floor_date(ymd_hms(fecha_corte), "month"))
      ) %>% 
      mutate(
        across(
          c(unicap, tipo_entidad, codigo_entidad), 
          as.numeric
        )
      ) %>% 
      # Agregando el diccionario de subproductos
      left_join(diccionario, by = join_by(unicap, desc_renglon)) %>% 
      filter(!is.na(modalidad)) %>% 
      # Agregando ID de entidades
      left_join(entidades, by = join_by(tipo_entidad, codigo_entidad)) %>% 
      # Dejando sólo tipos de interés
      dplyr::select(
        fecha, modalidad, subproducto, name, 9, 10
      ) %>% 
      # Renombrando
      setNames(., c(
        "fecha", "modalidad", "subproducto", "nombre_entidad",
        "Bruta", "Vigente")) %>% 
      mutate(
        across(
          c(Bruta, Vigente), 
          as.numeric
        )
      ) %>% 
      # Calculando la cartera Vencida
      mutate(
        Vencida = Bruta - Vigente
      ) %>% 
      dplyr::select(-Vigente) %>% 
      # Eliminando duplicados
      dplyr::distinct()
    
    # Llevando la data a formato long
    dc_long = dc_filtered %>%
      pivot_longer(
        cols = -c(1:4), 
        names_to = "tipo", values_to = "valor"
      ) %>% 
      mutate(
        # Miles de millones
        valor = valor/1e9 
      )
    
    # Cartera total
    dc_total_total = dc_long %>% 
      reframe(
        valor = sum(valor), 
        .by = c("fecha", "tipo")
      ) %>% 
      pivot_wider(
        names_from = tipo, values_from = valor
      ) %>% 
      rename_with(~ paste0(.x, "_total"), -fecha) %>% 
      janitor::clean_names()
    
    # Cartera total por modalidad
    dc_modalidad_total = dc_long %>% 
      group_by(fecha, tipo, modalidad) %>% 
      reframe(
        valor = sum(valor)
      ) %>% ungroup() %>% 
      pivot_wider(
        names_from = c(tipo, modalidad), values_from = valor
      ) %>% 
      janitor::clean_names()
    
    # Cartera total por subproductos
    dc_subproducto_total = dc_long %>% 
      group_by(fecha, tipo, modalidad, subproducto) %>% 
      reframe(
        valor = sum(valor)
      ) %>% ungroup() %>% 
      mutate(
        nombre_entidad = "Total" 
      )
    
    # Calculando agregado por subproducto y entidades
    dc_subproducto = dc_long %>% 
      group_by(fecha, nombre_entidad, tipo, modalidad, subproducto) %>% 
      reframe(
        valor = sum(valor)
      ) %>% 
      ungroup() %>% 
      # Agregando Total
      bind_rows(dc_subproducto_total)
    
    # Guardando información
    save(
      dc_subproducto,
      dc_total_total,
      dc_modalidad_total,
      file = current_path
    )
    
  } else {
    
    # Alerta por si la fecha traida es menor a la más reciente
    print(cli::rule(center = "❌ ¡ERROR!", line = "=", col = "#C30909"))
    
    cli::cli_alert_danger(
      "La data traida debe ser más reciente. Es {fetched_date} y la más reciente es {mostrecent_date}" 
    )
  }
}