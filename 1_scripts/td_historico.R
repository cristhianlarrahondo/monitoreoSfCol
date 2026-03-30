################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Histórico Tasas y Desembolsos

################################################################################
# Librerias
{
  library(tidyverse)
  library(lubridate)
  library(ggplot2)
  library(data.table)
  library(janitor)
}

rm(list = ls())

# # 1 Bancos, 2 Corporaciones financieras, 22 IOEs, 32 Cooperativas caracter financiero, 4 Compañias de financiamiento

## CR
# Sólo 1 Bancos, 2 Corporaciones financieras,
# 4 Compañías de financiamiento, 32 Cooperativas caracter financiero

################################################################################

# Cargando
#load("0_data/0_raw/tasas_working.RData")

old = fst::read_fst(
  path = "0_data/0_raw/td_historicos/old_api_tasas_desembolsos.fst",
  as.data.table = T
)

strata_cols = c(
  "Tipo_Entidad", "Nombre_Entidad", "Fecha_Corte", "Tipo_de_crédito",
  "Producto de crédito",
  "Tasa_efectiva_promedio_ponderada", "Montos_desembolsados",
  "Numero_de_creditos_desembolsados"
)

new = fst::read_fst(
  path = "0_data/0_raw/td_historicos/new_api_tasas_desembolsos.fst",
  as.data.table = T, columns = strata_cols
)


# Cargando la variación de la UVR
uvr_var = readRDS("0_data/1_processed/uvr_variation.Rds")

# Función para suma no NAs
sum_na = function(x) {
  if (all(is.na(x))) {
    NA
  } else {
    sum(x, na.rm = TRUE)
  }
}

################################################################################

# Antigua API. Desde mayo de 2002 a junio de 2022

## Cargando identificadores únicos de modalidad de crédito
id_modalidad_old = readxl::read_excel(
  path = "0_data/0_raw/diccionarios/diccionario_carteras_old_api.xlsx", 
  sheet = "V2"
)

## Transformaciones para consolidar la información
old_t = old %>% 
  # Limpiar nombre de columnas
  janitor::clean_names() %>% 
  mutate(
    # Variable mensual (los datos son semanales)
    fecha = floor_date(dmy(fecha_corte), "month"),
    montos = montos/1e3,
    # UVR y Pesos
    id_uvr = case_when(
      grepl("UVR", nombre_subcuenta, ignore.case = TRUE) ~ "UVR",
      grepl("PESOS", nombre_subcuenta, ignore.case = TRUE) ~ "Pesos",
      TRUE ~ NA_character_
    ),
    # VIS y No VIS
    id_vis = case_when(
      grepl("DIF", nombre_subcuenta, ignore.case = TRUE) ~ "No VIS",
      grepl("VIS", nombre_subcuenta, ignore.case = TRUE) ~ "VIS",
      TRUE ~ NA_character_
    )
  ) %>% 
  # Pegando identificador único de modalidades de crédito
  left_join(y = id_modalidad_old, by = join_by(nombre, tipo)) %>% 
  # Filtrando sólo por aquello que es identificado
  filter(
    !is.na(subproducto),
    # Sólo Bancos comerciales y CF para todo lo que no sea vivienda
    # Vivienda sí incluye todas las instituciones
    #(tipo_entidad %in% c(1, 4) & modalidad != "Hipotecaria") |
      #(modalidad == "Hipotecaria")
    (tipo_entidad %in% c(1, 2, 32, 4))
  ) %>% 
  # Renombrando variables
  rename(
    tasa = tasa_efectiva_anual,
    desembolsos = montos,
    n_creditos = num_creditos_desembolsados
  ) %>% 
  # Seleccionando solo columnas de interés
  dplyr::select(
    fecha, nombre_entidad,
    modalidad, subproducto, id_uvr, id_vis,
    tasa, desembolsos,
    n_creditos
  ) 

################################################################################

# Nueva API. A partir de julio de 2022 (Empalman perfecto)

## Cargando identificadores únicos de modalidad de crédito
id_modalidad_new = readxl::read_excel(
  path = "0_data/0_raw/diccionarios/diccionario_carteras_new_api.xlsx", 
  sheet = "V2"
)

# Definiendo como data.table
setDT(new)
setDT(id_modalidad_new)
setDT(uvr_var)

# Ajustando formato de fecha de la UVR (posictx a date)
uvr_var[, fecha := as.Date(fecha)]

# Paso 1: Limpiar nombre de columnas
new = setnames(new, janitor::make_clean_names(names(new)))

# Paso 2: Columnas de fechas (mensual y semanal)
new[, fecha := floor_date(dmy(fecha_corte), "month")]
new[, fecha2 := dmy(fecha_corte)]

# Paso 3: Crear modalidad4 para VIS y No VIS

# Paso 4: Pegar identificador de modalidades
new = id_modalidad_new[new, on = .(producto_de_credito, tipo_de_credito)]

# Paso 5: Filtrar sólo para datos identificados 
#         Sólo Bancos comerciales y CF para todo lo que no sea vivienda
#         Vivienda sí incluye todas las instituciones
new = new[
  !is.na(subproducto) &
    #((tipo_entidad %in% c(1, 4) & modalidad != "Hipotecaria") | modalidad == "Hipotecaria") &
    (tipo_entidad %in% c(1, 2, 32, 4)) &
    fecha < ymd("2025-04-01")
]

# Paso 6: Renombrar columnas
new[, tasa := tasa_efectiva_promedio_ponderada]
new[, desembolsos := montos_desembolsados/1e6]
new[, n_creditos := numero_de_creditos_desembolsados]

# Paso 7: Pegar UVR para ajustar por variación de UVR
#         Solo colocación en UVR y a partir del 29 de sep de 2023
new = uvr_var[new, on = .(fecha)]

# Paso 8: Ajustar tasa por UVR
new[, tasa := fcase(
  id_uvr == "UVR" & fecha2 >= ymd("2023-09-29") &
    !is.na(tasa) & !is.na(uvr_var),
  ((1 + tasa / 100) * (1 + uvr_var / 100) - 1) * 100,
  default = tasa
)][
  order(-fecha, modalidad)
]

# Paso 10: Reordenando
setorder(new, -fecha, nombre_entidad, modalidad)

cols_to_keep = colnames(old_t)

new_t = new[, ..cols_to_keep]

################################################################################

# Juntando las dos series
tasas_desembolsos = rbindlist(list(old_t, new_t), use.names = TRUE, fill = TRUE)

# saveRDS(
#   object = tasas_desembolsos,
#   file = "0_data/1_processed/historico/h_td_mar25.rds"
# )

# Calculate tasa_p
tasas_desembolsos[
  , tasa_p := tasa * desembolsos
][
  order(nombre_entidad, fecha, modalidad)
]

# Total
td_total = tasas_desembolsos[
  , .(
    desembolso_total = sum(desembolsos, na.rm = TRUE),
    part = sum(tasa_p, na.rm = TRUE),
    n_creditos = if (all(is.na(n_creditos))) NA else sum(n_creditos, na.rm = TRUE)
  ),
  by = .(fecha)
][
  , .(
    fecha,
    modalidad = "Total",
    desembolso_total,
    part,
    n_creditos,
    tasa_ponderada = part / desembolso_total
  )
][
  order(fecha)
]

# Modalidad
## Modalidad - Institución
td_mod_inst = tasas_desembolsos[
  , .(
    desembolso_total = sum(desembolsos, na.rm = TRUE),
    part = sum(tasa_p, na.rm = TRUE),
    n_creditos = if (all(is.na(n_creditos))) NA else sum(n_creditos, na.rm = TRUE)
  ),
  by = .(fecha, modalidad, nombre_entidad)
][
  , tasa_ponderada := part / desembolso_total
][
  order(fecha, nombre_entidad, modalidad)
]

# Modalidad
## Modalidad - Agregado
td_mod_agregada0 = tasas_desembolsos[
  , .(
    desembolso_total = sum(desembolsos, na.rm = TRUE),
    part = sum(tasa_p, na.rm = TRUE),
    n_creditos = if (all(is.na(n_creditos))) NA else sum(n_creditos, na.rm = TRUE)
  ),
  by = .(fecha, modalidad)
][
  , tasa_ponderada := part / desembolso_total
][
  order(fecha, modalidad)
]

td_mod_agregada = rbind(
  td_mod_agregada0,
  td_total,
  use.names = TRUE,
  fill = TRUE
)[
  order(fecha, modalidad)
]

# Subproducto
## Subproducto - Institución
td_inst = tasas_desembolsos[
  , .(
    desembolso_total = sum(desembolsos, na.rm = TRUE),
    part = sum(tasa_p, na.rm = TRUE),
    n_creditos = if (all(is.na(n_creditos))) NA else sum(n_creditos, na.rm = TRUE)
  ),
  by = .(fecha, modalidad, subproducto, nombre_entidad)
][
  , tasa_ponderada := part / desembolso_total
][
  order(fecha, modalidad, nombre_entidad, subproducto)
]

# Subproducto
## Subproducto - Agregado
td_agregada = tasas_desembolsos[
  , .(
    desembolso_total = sum(desembolsos, na.rm = TRUE),
    part = sum(tasa_p, na.rm = TRUE),
    n_creditos = if (all(is.na(n_creditos))) NA else sum(n_creditos, na.rm = TRUE)
  ),
  by = .(fecha, modalidad, subproducto)
][
  , tasa_ponderada := part / desembolso_total
][
  order(fecha, modalidad, subproducto)
]

################################################################################

# Exportando la data

save(
  td_mod_inst, td_mod_agregada, td_inst, td_agregada, 
  file = "0_data/1_processed/historico/td/td_historico_ene25V4.RData"
)