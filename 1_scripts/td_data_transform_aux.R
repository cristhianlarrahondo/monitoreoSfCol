################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Función auxiliar

################################################################################

# Tasas y desembolsos
td_function = function(data) {
  
  ##############################################################################
  
  # Función para suma no NAs
  sum_na = function(x) {
    if (all(is.na(x))) {
      NA
    } else {
      sum(x, na.rm = TRUE)
    }
  }
  
  # Manejo de los archivos nuevos y respaldos
  ## Ruta del td_mas_reciente actual
  current_path = "0_data/1_processed/mas_reciente/td/td_mas_reciente.RData"
  # Ruta donde se guarda el respaldo
  backup_dir = "0_data/1_processed/mas_reciente/td/respaldo"
  file_pattern = "td_mas_reciente_\\d{4}_\\d{2}\\.rds"
  
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

  # Histórico más reciente
  env = new.env()
  load(current_path, envir = env)
  
  # Access everything as a list
  td_mas_reciente = as.list(env)
  rm(env)
  
  # Fecha de la data traida de la API
  fetched_date = as.Date(unique(floor_date(ymd_hms(data$fecha_corte), "month")))
  
  # Fecha más reciente del archivo histórico
  mostrecent_date = max(unique(td_mas_reciente[["td_mod_agregada"]]$fecha))
  
  # Validaciones del proceso
  ## Que data (la traida de la API) sólo tenga un mes
  if (length(fetched_date) == 1) {
    
	 ##############################################################################
	
    ## Que la fecha de la data traida sea superior a la más reciente ya consolidada
    if (fetched_date > mostrecent_date) {
      
      expected_next_date = mostrecent_date %m+% months(1)
      
      ## Que además sea justamente el mes siguiente
      if (fetched_date == expected_next_date) {
        
        ########################################################################
        
        cli::cli_alert_success(
          "¡El mes descargado se actualizará! Continuando con el proceso..."
        )
        
        # Cargando archivos para el procesamiento  
        ## Cargando identificadores únicos de modalidad de crédito
        id_modalidad_new = readxl::read_excel(
          path = "0_data/0_raw/diccionarios/diccionario_carteras_new_api.xlsx", 
          sheet = "V2"
        ) %>% 
          rename(
            tipo_de_cr_dito = tipo_de_credito,
            producto_de_cr_dito = producto_de_credito
          )
        
        # Cargando la variación de la UVR
        source(file = "1_scripts/uvr_variation.R")
        uvr_var = readRDS("0_data/1_processed/uvr_variation.rds")
        
        
        # Sufijo para guardar archivos del mes
        suffix_filename = glue(
          "_{lubridate::year(expected_next_date)}_{sprintf('%02d',lubridate::month(expected_next_date))}"
        )
        
        # Nombre y carpeta del archivo cr más reciente guardado de respaldo
        # Antes que la actualización reemplace el actual
        # cr_mas_reciente.rds
        backup_filename = glue(
          "td_mas_reciente{suffix_filename}.Rdata" 
        )
        backup_path = file.path(backup_dir, backup_filename)
        ########################################################################
        
        cli::cli_alert_success(
          "Empezando con el tratamiento de los datos..."
        )
        
        ## Transformaciones para consolidar la información
        td_m = data %>% 
          # Limpiando nombre de las columnas
          janitor::clean_names() %>% 
          mutate(across(
            c(montos_desembolsados, tasa_efectiva_promedio, numero_de_creditos),
            as.numeric
          )) %>% 
          mutate(
            # Creando fecha mensual (datos son semanales)
            fecha = as.Date(floor_date(ymd_hms(fecha_corte), "month"))
          ) %>% 
          # Pegando identificador único de modalidades de crédito
          left_join(y = id_modalidad_new, by = join_by(producto_de_cr_dito, tipo_de_cr_dito)) %>% 
          filter(
            !is.na(subproducto),
            # Sólo 1 Bancos, 2 Corporaciones financieras,
            # 4 Compañías de financiamiento, 32 Cooperativas caracter financiero
            (tipo_entidad %in% c(1, 2, 32, 4))
          ) %>% 
          # Renombrando columnas
          rename(
            tasa = tasa_efectiva_promedio,
            desembolsos = montos_desembolsados,
            n_creditos = numero_de_creditos
          ) %>% 
          # Pegándole la variación de UVR
          left_join(uvr_var, by = join_by(fecha)) %>% 
          # Ajustando tasa de UVR (solo aquellas colocaciones en UVR)
          mutate(
            tasa = case_when(
              id_uvr == "UVR" & fecha >= ymd("2023-09-29") ~ 
                ((1 + tasa/100) * (1 + uvr_var/100) - 1) * 100,
              TRUE ~ tasa  
            ),
            # En miles
            desembolsos = desembolsos/1e6,
            tasa_p = tasa * desembolsos
          ) %>% 
          # Seleccionando variables de interés
          dplyr::select(
            fecha, nombre_entidad,
            modalidad, subproducto, id_uvr, id_vis,
            tasa, desembolsos,
            n_creditos, tasa_p
          ) %>%
          arrange(nombre_entidad, fecha, modalidad) %>% 
          mutate(
            fecha = as.Date(fecha)
          )
        
        # Guardando el archivo procesado del mes
        cli::cli_alert_success(
          "
          Archivo procesado del mes {.file td_{suffix_filename}.rds} guardado en {.path 0_data/1_processed/mensual/td/}
          "
        )
        saveRDS(
          object = td_m, 
          file = glue("0_data/1_processed/mensual/td/td_{suffix_filename}.rds")
        )
        
        ########################################################################
        # Cálculos intermedios
        
        resumir = function(data, ...) {
          data %>%
            group_by(...) %>%
            reframe(
              desembolso_total = sum(desembolsos, na.rm = TRUE),
              part = sum(tasa_p, na.rm = TRUE),
              n_creditos = sum_na(n_creditos)
            ) %>%
            ungroup() %>%
            mutate(tasa_ponderada = part / desembolso_total)
        }
        
        # Total
        td_total_m = resumir(td_m, fecha) %>% 
          mutate(
            modalidad = "Total"
          )
          
        # Modalidad - por institución
        td_mod_inst_m = resumir(td_m, fecha, nombre_entidad, modalidad)
        
        # Modalidad - agregada
        td_mod_agregada_m0 = resumir(td_m, fecha, modalidad)
        
        # Subproducto - por institución
        td_inst_m = resumir(td_m, fecha, nombre_entidad, modalidad, subproducto)
        
        # Subproducto - agregada
        td_agregada_m = resumir(td_m, fecha, modalidad, subproducto)
        
        ########################################################################
        # Agregando al histórico más reciente para tener el nuevo más reciente
        td_mod_agregada_m = rbind(td_mod_agregada_m0, td_total_m)
        
        td_mas_reciente_append = list(
          td_mod_agregada = rbind(
            td_mas_reciente$td_mod_agregada, td_mod_agregada_m
          ),
          td_mod_inst = rbind(
            td_mas_reciente$td_mod_inst, td_mod_inst_m
          ),
          td_inst = rbind(
            td_mas_reciente$td_inst, td_inst_m
          ),
          td_agregada = rbind(
            td_mas_reciente$td_agregada, td_agregada_m
          )
        )
        
        # Moviendo el archivo más reciente a la carpeta de respaldo 
        if (file_exists(current_path)) {
          file_move(current_path, backup_path)
		  
          cli::cli_alert_info(
            "Antiguo archivo {.file td_mas_reciente.rds} guardado como respaldo en: {.path 0_data/1_processed/mas_reciente/td/respaldo}"
          )
          
        } else {
          cli::cli_alert_warning("No existe un archivo {.file td_mas_reciente.rds} para respaldar.")
        }
        
        # Guardando la nueva versión más reciente
        list2env(td_mas_reciente_append, envir = environment())
        save(
          td_mod_inst, td_mod_agregada, td_inst, td_agregada,
          file = current_path
        )
        
        print(cli::rule(center = "🎉 ¡Los datos han sido actualizados!", line = "=", col = "green"))
        cli::cli_alert_success(
          "El archivo nuevo {.file td_mas_reciente.rds} guardado en: {.path 0_data/1_processed/mas_reciente/td/}"
        )
        
        # Eliminando respaldos antiguos (solo se dejan los últimos 3 meses)
        file_delete(files_to_delete)
        
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
      
      td_t = as.list(load(file = "0_data/1_processed/mas_reciente/td/td_mas_reciente.RData"))
      
    } else {  # La fecha traida es menor a la más reciente
      
      print(cli::rule(center = "🚧 ¡CUIDADO! La fecha de la data traida es menor a la más reciente.", col = "#FFC552"))
      
      cli::cli_alert_danger(
        "
        Revisar la fecha de la data traida. 
        Debe ser el mes siguiente a la más reciente en el archivo histórico, la cual es: 
        {mostrecent_date}, pero es {fetched_date}
        "
      )
      
    }
  } else { # Si data traida tiene más de un mes
    
    print(cli::rule(center = "❌ ¡ERROR!", line = "=", col = "#C30909"))
    
    cli::cli_alert_danger(
      "La data traida debe tener solo un mes, no más de uno a la vez."
    )
    
  }
  
}
################################################################################