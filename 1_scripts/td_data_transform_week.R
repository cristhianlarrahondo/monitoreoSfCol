################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Función auxiliar

################################################################################

# Tasas y desembolsos
td_function_week = function(data, filename) {
  
  ##############################################################################
  
  # Función para suma no NAs
  sum_na = function(x) {
    if (all(is.na(x))) {
      NA
    } else {
      sum(x, na.rm = TRUE)
    }
  }
        
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
  #source(file = "1_scripts/uvr_variation.R")
  uvr_var = readRDS("0_data/1_processed/uvr_variation.rds")
  
  ## Transformaciones para consolidar la información
  td_m = data %>% 
    # Limpiando nombre de las columnas
    janitor::clean_names() %>% 
    mutate(across(
      c(montos_desembolsados, tasa_efectiva_promedio, numero_de_creditos),
      as.numeric
    )) %>% 
    mutate(
      # Ajustando se mantiene la semanal
      fecha = as.Date(floor_date(ymd_hms(fecha_corte), "month")),
      fecha_s = as.Date(lubridate::ymd_hms(fecha_corte))
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
      fecha_s, nombre_entidad,
      modalidad, subproducto, id_uvr, id_vis,
      tasa, desembolsos,
      n_creditos, tasa_p
    ) %>%
    rename(fecha = fecha_s) %>% 
    arrange(nombre_entidad, fecha, modalidad) %>% 
    mutate(
      fecha = as.Date(fecha)
    )
  
  # saveRDS(
  #   object = td_m, 
  #   file = glue("0_data/2_final/request/{filename}.rds")
  # )
  
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
  
  
  # Saving
  save(
    td_mod_inst_m, td_mod_agregada_m, td_inst_m, td_agregada_m,
    file = glue("0_data/2_final/request/{filename}.Rdata")
  )
}
################################################################################


