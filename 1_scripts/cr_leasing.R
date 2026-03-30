################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Agosto 2025
################################################################################

# Leasing histórico 
## Descarga toda la serie de leasing habitacional desde datos abiertos

cli::cli_process_start(
  "Inicio actualización de la información de leasing habitacional."
)

################################################################################

## Leasing habitacional bancos (Sólo establecimientos bancarios)
## Leasing habitacional compañías de financiamiento
## 140410 CATEGORÍA A RIESGO NORMAL LEASING HABITACIONAL
## 140420 CATEGORÍA B RIESGO ACEPTABLE LEASING HABITACIONAL
## 140430 CATEGORÍA C RIESGO APRECIABLE LEASING HABITACIONAL
## Por fuera riesgo significativo e incobrabilidad
## 149805 VIVIENDA Y LEASING HABITACIONAL
## 160810 CATEGORÍA A RIESGO NORMAL, LEASING HABITACIONAL
## 160812 CATEGORÍA B RIESGO ACEPTABLE, LEASING HABITACIONAL
## 160814 CATEGORÍA C RIESGO APRECIABLE, LEASING HABITACIONAL
## Por fuera significativo, incobrabilidad
## 170235 BIENES INMUEBLES EN LEASING HABITACIONAL

library(httr)
library(jsonlite)

#url = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$query=SELECT%0A%20%20%60tipo_entidad%60%2C%0A%20%20%60nombre_tipo_entidad%60%2C%0A%20%20%60codigo_entidad%60%2C%0A%20%20%60nombre_entidad%60%2C%0A%20%20%60fecha_corte%60%2C%0A%20%20%60cuenta%60%2C%0A%20%20%60nombre_cuenta%60%2C%0A%20%20%60moneda%60%2C%0A%20%20%60nombre_moneda%60%2C%0A%20%20%60signo_valor%60%2C%0A%20%20%60valor%60%0AWHERE%0A%20%20caseless_one_of(%0A%20%20%20%20%60cuenta%60%2C%0A%20%20%20%20%22%22%2C%0A%20%20%20%20%22140410%22%2C%0A%20%20%20%20%22140420%22%2C%0A%20%20%20%20%22140430%22%2C%0A%20%20%20%20%22149805%22%2C%0A%20%20%20%20%22160810%22%2C%0A%20%20%20%20%22160812%22%2C%0A%20%20%20%20%22160814%22%2C%0A%20%20%20%20%22170235%22%0A%20%20)%0ALIMIT%20100000000"
#url = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$query=SELECT%0A%20%20%60tipo_entidad%60%2C%0A%20%20%60nombre_tipo_entidad%60%2C%0A%20%20%60codigo_entidad%60%2C%0A%20%20%60nombre_entidad%60%2C%0A%20%20%60fecha_corte%60%2C%0A%20%20%60cuenta%60%2C%0A%20%20%60nombre_cuenta%60%2C%0A%20%20%60moneda%60%2C%0A%20%20%60nombre_moneda%60%2C%0A%20%20%60signo_valor%60%2C%0A%20%20%60valor%60%0AWHERE%0A%20%20caseless_one_of(%60nombre_moneda%60%2C%20%22Total%22)%0A%20%20AND%20%60tipo_entidad%60%20IN%20(%221%22%2C%20%224%22)%0A%20%20AND%20%60cuenta%60%20IN%20(%0A%20%20%20%20%22%22%2C%0A%20%20%20%20%22140410%22%2C%0A%20%20%20%20%22140420%22%2C%0A%20%20%20%20%22140430%22%2C%0A%20%20%20%20%22149805%22%2C%0A%20%20%20%20%22160810%22%2C%0A%20%20%20%20%22160812%22%2C%0A%20%20%20%20%22160814%22%2C%0A%20%20%20%20%22170235%22%0A%20%20)%0ALIMIT%20100000000"

base_url <- "https://www.datos.gov.co/resource/mxk5-ce6w.json"
api_token <- "ksrkPBZQyuodkKKSyDFggj3BG"

# Filtros
tipos_entidad <- c("1", "4")
cuentas <- c(
  "",
  "140410", "140420", "140430",
  "149805",
  "160810", "160812", "160814",
  "170235"
)

# Helper para construir listas tipo SQL: ("1", "4")
sql_in <- function(x) {
  paste0('("', paste(x, collapse = '", "'), '")')
}

# Consulta SoQL legible
soql_query <- glue::glue("
SELECT
  `tipo_entidad`,
  `nombre_tipo_entidad`,
  `codigo_entidad`,
  `nombre_entidad`,
  `fecha_corte`,
  `cuenta`,
  `nombre_cuenta`,
  `moneda`,
  `nombre_moneda`,
  `signo_valor`,
  `valor`
WHERE
  `tipo_entidad` IN {sql_in(tipos_entidad)}
  AND caseless_one_of(`nombre_moneda`, \"Total\")
  AND `cuenta` IN {sql_in(cuentas)}
")

# Request
response <- GET(
  url = base_url,
  query = list(`$query` = soql_query),
  add_headers(`X-App-Token` = api_token)
)

# Validar respuesta
stop_for_status(response)

# Parsear a data frame
data <- content(response, as = "text", encoding = "UTF-8") |> 
  fromJSON(flatten = TRUE)

# Guardando el archivo
saveRDS(
  object = data, 
  file = "0_data/0_raw/pulled/leasing/leasing_mas_reciente.rds"
)

leasing_mas_reciente = data %>% 
  filter(
    moneda == "0",
    # Sólo Bancos y CFs
    tipo_entidad %in% c("1", "4")
  ) %>%
  mutate(
    valor = as.numeric(valor),
    fecha = floor_date(ymd_hms(fecha_corte), "month"),
    macrocuenta = ifelse(tipo_entidad == "1", "Leasing_Bancos", "Leasing_CF")
  ) %>% 
  group_by(fecha, macrocuenta) %>% 
  reframe(
    # En millones
    valor = sum(valor, na.rm = T)/1e6
  ) %>% ungroup() %>% 
  mutate(
    tipo = "Bruta"
  )

# Guardando el archivo
saveRDS(
  object = leasing_mas_reciente, 
  file = "0_data/1_processed/mas_reciente/leasing/leasing_mas_reciente.rds"
)

################################################################################
# Mensaje de final
last_month = data %>% 
  pull(fecha_corte) %>% 
  as.Date() %>% max()

cli::cli_alert_success(
  "
  Se ha actualizado correctamente la información de leasing habitacional.
  La información está a corte del mes: {last_month}
  "
)

cli::cli_process_done("Terminada la actualización.")

################################################################################

